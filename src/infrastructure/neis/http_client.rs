//! 재시도 + 백오프가 내장된 HTTP 클라이언트.
//!
//! - 기본 재시도 대상 status: `{429, 500, 502, 503, 504}`
//! - 백오프: `base * 2^attempt + random(0, base)`
//! - `Retry-After` 헤더 존중 (정수 초 또는 HTTP-date)
//! - 동시성 한도: keepalive 10 / 전체 20

use std::time::Duration;

use chrono::{DateTime, Utc};
use rand::RngExt;
use reqwest::header::{HeaderMap, RETRY_AFTER};
use reqwest::{Client, Response, StatusCode, Url};
use tokio::time::sleep;

use crate::error::{HDMealError, HDMealResult};
use crate::shared::tls;

const DEFAULT_BASE_DELAY_MS: u64 = 500;
const DEFAULT_MAX_DELAY_MS: u64 = 30_000;
const DEFAULT_MAX_RETRIES: u32 = 2;
const DEFAULT_RETRY_STATUSES: &[u16] = &[429, 500, 502, 503, 504];

/// 재시도 옵션.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub retry_statuses: Vec<StatusCode>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            base_delay: Duration::from_millis(DEFAULT_BASE_DELAY_MS),
            max_delay: Duration::from_millis(DEFAULT_MAX_DELAY_MS),
            retry_statuses: DEFAULT_RETRY_STATUSES
                .iter()
                .copied()
                .filter_map(|c| StatusCode::from_u16(c).ok())
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HttpClient {
    inner: Client,
    policy: RetryPolicy,
}

impl HttpClient {
    pub fn new() -> HDMealResult<Self> {
        // reqwest `rustls-no-provider` 는 process-wide default provider 를 필요로 합니다.
        // 앱 시작 경로에서 이미 설치되었더라도 이 호출은 안전합니다.
        tls::install_rustls_ring_provider();

        let inner = reqwest::Client::builder()
            .pool_max_idle_per_host(10)
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(5))
            .gzip(true)
            .build()
            .map_err(HDMealError::from)?;
        Ok(Self {
            inner,
            policy: RetryPolicy::default(),
        })
    }

    pub fn with_policy(policy: RetryPolicy) -> HDMealResult<Self> {
        let mut s = Self::new()?;
        s.policy = policy;
        Ok(s)
    }

    pub fn inner(&self) -> &Client {
        &self.inner
    }

    /// GET 요청을 보내고 JSON 을 디시리얼라이즈.
    pub async fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str) -> HDMealResult<T> {
        let resp = self.get_with_retry(url, HeaderMap::new()).await?;
        let value = resp.json::<T>().await?;
        Ok(value)
    }

    /// 쿼리 파라미터를 URL 에 추가해 GET.
    pub async fn get_json_with_params<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        params: &[(&str, String)],
    ) -> HDMealResult<T> {
        let url = Url::parse_with_params(url, params.iter().map(|(k, v)| (*k, v.as_str())))?;
        let resp = self
            .get_with_retry(url.as_str(), reqwest::header::HeaderMap::new())
            .await?;
        let val = resp.json::<T>().await?;
        Ok(val)
    }

    /// GET 요청을 재시도와 함께 보냅니다.
    pub async fn get_with_retry(
        &self,
        url: &str,
        extra_headers: HeaderMap,
    ) -> HDMealResult<Response> {
        let mut attempt = 0u32;
        loop {
            let mut req = self.inner.get(url);
            for (k, v) in extra_headers.iter() {
                req = req.header(k, v);
            }
            let resp = req.send().await;

            match resp {
                Ok(r) if self.policy.retry_statuses.contains(&r.status()) => {
                    if attempt >= self.policy.max_retries {
                        return Ok(r);
                    }
                    let delay = self.compute_delay(&r, attempt);
                    tracing::warn!(
                        url = %sanitize_url(url),
                        status = %r.status(),
                        attempt = attempt + 1,
                        "retrying after status"
                    );
                    drop(r);
                    sleep(delay).await;
                    attempt += 1;
                }
                Ok(r) => return Ok(r),
                Err(e) => {
                    if attempt >= self.policy.max_retries {
                        return Err(HDMealError::Http(e));
                    }
                    tracing::warn!(
                        url = %sanitize_url(url),
                        error = %describe_reqwest_error(&e),
                        attempt = attempt + 1,
                        "retrying after error"
                    );
                    let delay = self.compute_delay_no_header(attempt);
                    sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }

    fn compute_delay(&self, resp: &Response, attempt: u32) -> Duration {
        if let Some(v) = resp.headers().get(RETRY_AFTER) {
            if let Ok(s) = v.to_str() {
                if let Ok(secs) = s.parse::<u64>() {
                    return cap_duration(Duration::from_secs(secs), self.policy.max_delay);
                }
                if let Some(delay) = retry_after_http_date_delay(s, Utc::now()) {
                    return cap_duration(delay, self.policy.max_delay);
                }
            }
        }
        self.compute_delay_no_header(attempt)
    }

    fn compute_delay_no_header(&self, attempt: u32) -> Duration {
        let base = self
            .policy
            .base_delay
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        let multiplier = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
        let exp = base.saturating_mul(multiplier);
        let jitter = rand::rng().random_range(0..base.max(1));
        cap_duration(
            Duration::from_millis(exp.saturating_add(jitter)),
            self.policy.max_delay,
        )
    }
}

pub(crate) fn describe_reqwest_error(err: &reqwest::Error) -> String {
    let mut message = err.to_string();
    if let Some(url) = err.url() {
        message = message.replace(url.as_str(), &sanitize_url(url.as_str()));
    }
    message
}

fn retry_after_http_date_delay(value: &str, now: DateTime<Utc>) -> Option<Duration> {
    let retry_at = DateTime::parse_from_rfc2822(value)
        .ok()?
        .with_timezone(&Utc);
    let delay = retry_at.signed_duration_since(now);
    delay.to_std().ok().or(Some(Duration::ZERO))
}

fn cap_duration(delay: Duration, max_delay: Duration) -> Duration {
    delay.min(max_delay)
}

pub(crate) fn sanitize_url(raw: &str) -> String {
    let Ok(mut url) = Url::parse(raw) else {
        return "<invalid url>".to_string();
    };
    let pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(k, v)| {
            let value = if is_sensitive_query_key(&k) {
                "<redacted>".to_string()
            } else {
                v.into_owned()
            };
            (k.into_owned(), value)
        })
        .collect();
    if !pairs.is_empty() {
        url.query_pairs_mut().clear().extend_pairs(
            pairs
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
    }
    redact_sensitive_path_segments(&mut url);
    url.into()
}

fn redact_sensitive_path_segments(url: &mut Url) {
    if url.host_str() != Some("openapi.seoul.go.kr") {
        return;
    }
    let Some(mut segments) = url
        .path_segments()
        .map(|segments| segments.map(str::to_owned).collect::<Vec<_>>())
    else {
        return;
    };
    if segments.get(1).map(String::as_str) != Some("json") {
        return;
    }
    if let Some(first) = segments.first_mut() {
        *first = "<redacted>".to_string();
    }
    if let Ok(mut path) = url.path_segments_mut() {
        path.clear().extend(segments.iter().map(String::as_str));
    }
}

fn is_sensitive_query_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "key" | "servicekey" | "token" | "apikey" | "api_key"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_grows_exponentially_with_jitter() {
        let c = HttpClient::new().unwrap();
        let d0 = c.compute_delay_no_header(0).as_millis();
        let d2 = c.compute_delay_no_header(2).as_millis();
        // attempt=0: 500 + [0,500) = [500, 1000)
        // attempt=2: 2000 + [0,500) = [2000, 2500)
        assert!((500..1000).contains(&d0), "got {d0}");
        assert!((2000..2500).contains(&d2), "got {d2}");
    }

    #[test]
    fn sanitize_url_redacts_known_secret_query_keys() {
        let url =
            sanitize_url("https://example.test/path?KEY=neis&serviceKey=kma&foo=bar&token=tok");
        assert!(url.contains("KEY=%3Credacted%3E"), "{url}");
        assert!(url.contains("serviceKey=%3Credacted%3E"), "{url}");
        assert!(url.contains("token=%3Credacted%3E"), "{url}");
        assert!(url.contains("foo=bar"), "{url}");
        assert!(!url.contains("neis"), "{url}");
        assert!(!url.contains("kma"), "{url}");
    }

    #[test]
    fn sanitize_url_redacts_seoul_open_data_path_token() {
        let url = sanitize_url(
            "https://openapi.seoul.go.kr:8088/secret-token/json/WPOSInformationTime/1/5/",
        );

        assert!(
            url.contains("/%3Credacted%3E/json/WPOSInformationTime/1/5/"),
            "{url}"
        );
        assert!(!url.contains("secret-token"), "{url}");
    }

    #[test]
    fn retry_delay_is_capped() {
        let c = HttpClient::with_policy(RetryPolicy {
            max_retries: 2,
            base_delay: Duration::from_secs(10),
            max_delay: Duration::from_millis(750),
            retry_statuses: vec![],
        })
        .unwrap();

        assert_eq!(c.compute_delay_no_header(10), Duration::from_millis(750));
    }

    #[test]
    fn retry_after_http_date_uses_future_delta() {
        let now = DateTime::parse_from_rfc2822("Wed, 21 Oct 2015 07:28:00 GMT")
            .unwrap()
            .with_timezone(&Utc);
        let delay = retry_after_http_date_delay("Wed, 21 Oct 2015 07:28:05 GMT", now).unwrap();

        assert_eq!(delay, Duration::from_secs(5));
    }

    #[test]
    fn retry_after_http_date_in_the_past_is_zero() {
        let now = DateTime::parse_from_rfc2822("Wed, 21 Oct 2015 07:28:05 GMT")
            .unwrap()
            .with_timezone(&Utc);
        let delay = retry_after_http_date_delay("Wed, 21 Oct 2015 07:28:00 GMT", now).unwrap();

        assert_eq!(delay, Duration::ZERO);
    }
}
