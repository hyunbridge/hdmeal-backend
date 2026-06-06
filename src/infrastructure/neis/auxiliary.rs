//! 보조 외부 API: KMA 단기예보 + 서울 한강 수온.
//!
//! - [`KmaClient::fetch_weather`]: 오늘 09:00 슬롯을 우선으로 TMP/SKY/PTY/POP/REH/TMN/TMX 수집.
//! - [`SeoulWaterClient::fetch`]: 한강 수온 평균.
//!
//! 둘 다 KST 자정 기준 timestamp 로 [`crate::domain`] 도큐먼트로 저장.

use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use serde::Deserialize;
use url::Url;

use super::http_client::HttpClient;
use crate::config::AppConfig;
use crate::domain::WeatherDocument;
use crate::error::{HDMealError, HDMealResult};
use crate::shared::timezone::KST;

const KMA_BASE_URL: &str =
    "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst";

/// KMA 발표 시각 (HH).
const KMA_BASE_TIMES: &[u32] = &[2, 5, 8, 11, 14, 17, 20, 23];

/// KMA 단기예보 fetch 결과.
#[derive(Debug, Clone)]
pub struct WeatherView {
    pub timestamp: DateTime<Utc>,
    pub temp: String,
    pub temp_min: String,
    pub temp_max: String,
    pub sky: String,
    pub pty: String,
    pub precip_probability: String,
    pub humidity: String,
    pub first_hour: String,
}

impl WeatherView {
    pub fn into_document(self) -> WeatherDocument {
        let now = Utc::now();
        WeatherDocument {
            id: format!("weather-{}", self.timestamp.timestamp()),
            timestamp: self.timestamp,
            temp: self.temp,
            temp_min: self.temp_min,
            temp_max: self.temp_max,
            sky: self.sky,
            pty: self.pty,
            precip_probability: self.precip_probability,
            humidity: self.humidity,
            first_hour: self.first_hour,
            created_at: now,
        }
    }
}

/// KMA 클라이언트.
#[derive(Clone)]
pub struct KmaClient {
    http: HttpClient,
    config: AppConfig,
}

impl KmaClient {
    pub fn new(config: AppConfig, http: HttpClient) -> Self {
        Self { http, config }
    }

    pub async fn fetch_weather(&self) -> HDMealResult<WeatherView> {
        let (base_date, base_time) = compute_base_time(Utc::now());
        let params = vec![
            ("serviceKey", self.config.kma_api_key.clone()),
            ("pageNo", "1".to_string()),
            ("numOfRows", "1000".to_string()),
            ("dataType", "JSON".to_string()),
            ("base_date", base_date.clone()),
            ("base_time", base_time.clone()),
            ("nx", self.config.kma_nx.to_string()),
            ("ny", self.config.kma_ny.to_string()),
        ];
        let resp: KmaResponse = self
            .http
            .get_json_with_params(KMA_BASE_URL, &params)
            .await?;
        let header = resp
            .response
            .header
            .as_ref()
            .ok_or_else(|| HDMealError::service_unavailable("KMA: missing header"))?;
        if header.result_code.as_deref() != Some("00") {
            return Err(HDMealError::service_unavailable(format!(
                "KMA error: {}",
                header.result_msg.as_deref().unwrap_or("unknown")
            )));
        }
        let items = resp
            .response
            .body
            .as_ref()
            .and_then(|b| b.items.as_ref())
            .map(|i| i.item.clone())
            .unwrap_or_default();

        // (1) 대표 슬롯: (today KST 09:00) → (tomorrow KST 09:00) → 첫 TMP
        let today = crate::shared::timezone::today_kst_date();
        let slot = pick_representative_slot(&items, today)
            .ok_or_else(|| HDMealError::service_unavailable("KMA: no items returned"))?;
        let (fcst_date, fcst_time) = slot;
        let ts: DateTime<Utc> = parse_fcst_dt(&fcst_date, &fcst_time)
            .ok_or_else(|| HDMealError::service_unavailable("KMA: bad fcst dt"))?;

        // (2) 카테고리별 값 추출
        let mut temp = String::new();
        let mut sky = String::new();
        let mut pty = String::new();
        let mut pop = String::new();
        let mut reh = String::new();
        let mut tmn = String::new();
        let mut tmx = String::new();
        let mut first_hour = String::new();
        for it in &items {
            if it.fcst_date != fcst_date {
                continue;
            }
            match it.category.as_str() {
                "TMP" if it.fcst_time == fcst_time => {
                    temp = it.fcst_value.clone();
                    if first_hour.is_empty() {
                        first_hour = it.fcst_time[0..2].to_string();
                    }
                }
                "SKY" if it.fcst_time == fcst_time => sky = map_sky(&it.fcst_value).to_string(),
                "PTY" if it.fcst_time == fcst_time => pty = map_pty(&it.fcst_value).to_string(),
                "POP" if it.fcst_time == fcst_time => pop = it.fcst_value.clone(),
                "REH" if it.fcst_time == fcst_time => reh = it.fcst_value.clone(),
                "TMN" if it.fcst_time == "0600" => tmn = it.fcst_value.clone(),
                "TMX" if it.fcst_time == "1500" => tmx = it.fcst_value.clone(),
                _ => {}
            }
        }

        // (3) TMN/TMX 가 비어 있으면 그날 다른 시간의 값으로 채운다 (fallback).
        if tmn.is_empty() {
            tmn = items
                .iter()
                .find(|i| i.category == "TMN" && i.fcst_date == fcst_date)
                .map(|i| i.fcst_value.clone())
                .unwrap_or_else(|| "-".to_string());
        }
        if tmx.is_empty() {
            tmx = items
                .iter()
                .find(|i| i.category == "TMX" && i.fcst_date == fcst_date)
                .map(|i| i.fcst_value.clone())
                .unwrap_or_else(|| "-".to_string());
        }

        Ok(WeatherView {
            timestamp: ts,
            temp,
            temp_min: tmn,
            temp_max: tmx,
            sky,
            pty,
            precip_probability: pop,
            humidity: reh,
            first_hour,
        })
    }
}

fn map_sky(v: &str) -> &'static str {
    match v {
        "1" => "☀ 맑음",
        "3" => "🌥️ 구름 많음",
        "4" => "☁ 흐림",
        _ => "Unknown",
    }
}

fn map_pty(v: &str) -> &'static str {
    match v {
        "0" => "❌ 없음",
        "1" => "🌧️ 비",
        "2" => "🌨️ 비/눈",
        "3" => "🌨️ 눈",
        "4" => "🚿 소나기",
        _ => "⚠ 오류",
    }
}

fn compute_base_time(now_utc: DateTime<Utc>) -> (String, String) {
    use chrono::Timelike;
    let kst = now_utc.with_timezone(&KST);
    let hour = kst.hour();
    let minute = kst.minute();

    // HH:10 KST 가 지나야 해당 base_time 의 데이터가 사용 가능.
    let mut base_hour = 23u32;
    for &h in KMA_BASE_TIMES.iter().rev() {
        if hour > h || (hour == h && minute >= 10) {
            base_hour = h;
            break;
        }
    }
    if base_hour == 23 && (hour < 2 || (hour == 2 && minute < 10)) {
        // 02:10 이전이면 어제 23:00 발표분 사용.
        let yesterday = kst.date_naive() - Duration::days(1);
        return (yesterday.format("%Y%m%d").to_string(), "2300".to_string());
    }
    (
        kst.format("%Y%m%d").to_string(),
        format!("{base_hour:02}00"),
    )
}

fn pick_representative_slot(items: &[KmaItem], today: NaiveDate) -> Option<(String, String)> {
    let today_str = today.format("%Y%m%d").to_string();
    if items
        .iter()
        .any(|i| i.fcst_date == today_str && i.fcst_time == "0900")
    {
        return Some((today_str, "0900".to_string()));
    }
    if Utc::now().with_timezone(&KST).hour() >= 17 {
        let tomorrow = today + Duration::days(1);
        let tomorrow_str = tomorrow.format("%Y%m%d").to_string();
        if items
            .iter()
            .any(|i| i.fcst_date == tomorrow_str && i.fcst_time == "0900")
        {
            return Some((tomorrow_str, "0900".to_string()));
        }
    }
    items
        .iter()
        .find(|i| i.category == "TMP")
        .map(|i| (i.fcst_date.clone(), i.fcst_time.clone()))
}

fn parse_fcst_dt(date: &str, time: &str) -> Option<DateTime<Utc>> {
    if date.len() != 8 || time.len() != 4 {
        return None;
    }
    let y = date[0..4].parse().ok()?;
    let m = date[4..6].parse().ok()?;
    let d = date[6..8].parse().ok()?;
    let h = time[0..2].parse().ok()?;
    let mi = time[2..4].parse().ok()?;
    let ndt = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(y, m, d)?,
        NaiveTime::from_hms_opt(h, mi, 0)?,
    );
    Some(KST.from_local_datetime(&ndt).unwrap().with_timezone(&Utc))
}

// ----------------- KMA DTOs -----------------

#[derive(Debug, Deserialize)]
struct KmaResponse {
    response: KmaResponseInner,
}

#[derive(Debug, Deserialize)]
struct KmaResponseInner {
    header: Option<KmaHeader>,
    body: Option<KmaBody>,
}

#[derive(Debug, Deserialize)]
struct KmaHeader {
    #[serde(rename = "resultCode")]
    result_code: Option<String>,
    #[serde(rename = "resultMsg")]
    result_msg: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KmaBody {
    items: Option<KmaItems>,
}

#[derive(Debug, Deserialize)]
struct KmaItems {
    item: Vec<KmaItem>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct KmaItem {
    #[serde(rename = "baseDate")]
    base_date: String,
    #[serde(rename = "baseTime")]
    base_time: String,
    #[serde(rename = "category")]
    category: String,
    #[serde(rename = "fcstDate")]
    fcst_date: String,
    #[serde(rename = "fcstTime")]
    fcst_time: String,
    #[serde(rename = "fcstValue")]
    fcst_value: String,
    #[serde(rename = "nx")]
    nx: i32,
    #[serde(rename = "ny")]
    ny: i32,
}

// ----------------- Seoul water temperature -----------------

const SEOUL_HTTPS_URL_TMPL: &str = "https://openapi.seoul.go.kr:8088";
const SEOUL_HTTP_URL_TMPL: &str = "http://openapi.seoul.go.kr:8088";

#[derive(Debug, Clone)]
pub struct SeoulWaterClient {
    http: HttpClient,
    token: String,
}

impl SeoulWaterClient {
    pub fn new(http: HttpClient, token: String) -> Self {
        Self { http, token }
    }

    /// 한강 수온. HTTPS 먼저 (재시도 없음), 실패 시 HTTP.
    pub async fn fetch(&self) -> HDMealResult<SeoulWaterReading> {
        let https_url = seoul_water_url(SEOUL_HTTPS_URL_TMPL, &self.token)?;
        let http_url = seoul_water_url(SEOUL_HTTP_URL_TMPL, &self.token)?;

        let primary = self
            .http
            .inner()
            .get(&https_url)
            .send()
            .await
            .and_then(|r| r.error_for_status());
        let resp = match primary {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "seoul water https failed, falling back to http");
                let r = self
                    .http
                    .inner()
                    .get(&http_url)
                    .send()
                    .await
                    .map_err(HDMealError::from)?;
                if !r.status().is_success() {
                    return Err(HDMealError::service_unavailable(format!(
                        "Seoul water http status {}",
                        r.status()
                    )));
                }
                r
            }
        };
        let body: SeoulWaterResponse = resp.json().await?;
        let rows = body
            .wpos_information_time
            .as_ref()
            .and_then(|w| w.row.clone())
            .ok_or_else(|| HDMealError::service_unavailable("Seoul water: no rows"))?;
        if rows.is_empty() {
            return Err(HDMealError::service_unavailable("Seoul water: empty rows"));
        }
        let first = &rows[0];
        // YMD = "2024-03-01", HR = "15"
        let ts = parse_seoul_water_timestamp(&first.YMD, &first.HR)?;

        let mut sum = 0.0f64;
        let mut count = 0u32;
        for r in &rows {
            if let Ok(v) = r.WATT.trim().parse::<f64>() {
                sum += v;
                count += 1;
            }
        }
        if count == 0 {
            return Err(HDMealError::service_unavailable("Seoul water: no WATT"));
        }
        let avg = (sum / count as f64 * 100.0).round() / 100.0;
        Ok(SeoulWaterReading {
            timestamp: ts,
            temperature_c: avg,
        })
    }
}

fn seoul_water_url(base: &str, token: &str) -> HDMealResult<String> {
    let mut url = Url::parse(base)?;
    url.path_segments_mut()
        .map_err(|_| HDMealError::internal("Seoul water: invalid base URL"))?
        .extend([token, "json", "WPOSInformationTime", "1", "5", ""]);
    Ok(url.into())
}

/// Seoul 수온 API 의 `YMD` (`"2024-03-01"`) + `HR` (`"15"`) → KST 자정 기준
/// 측정 시각의 UTC `DateTime`. `HR` 시간 정각, 분/초 = 0 으로 가정.
fn parse_seoul_water_timestamp(ymd: &str, hr: &str) -> HDMealResult<DateTime<Utc>> {
    let ndt = NaiveDateTime::parse_from_str(&format!("{ymd} {hr}:00:00"), "%Y-%m-%d %H:%M:%S")
        .map_err(|_| HDMealError::service_unavailable("Seoul water: bad YMD/HR"))?;
    let ts = KST
        .from_local_datetime(&ndt)
        .single()
        .ok_or_else(|| HDMealError::service_unavailable("Seoul water: ambiguous local time"))?
        .with_timezone(&Utc);
    Ok(ts)
}

#[derive(Debug, Clone)]
pub struct SeoulWaterReading {
    pub timestamp: DateTime<Utc>,
    pub temperature_c: f64,
}

#[derive(Debug, Deserialize)]
struct SeoulWaterResponse {
    #[serde(rename = "WPOSInformationTime")]
    wpos_information_time: Option<SeoulWaterInfo>,
}

#[derive(Debug, Deserialize)]
struct SeoulWaterInfo {
    row: Option<Vec<SeoulWaterRow>>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
struct SeoulWaterRow {
    YMD: String,
    HR: String,
    WATT: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sky_mapping() {
        assert_eq!(map_sky("1"), "☀ 맑음");
        assert_eq!(map_sky("3"), "🌥️ 구름 많음");
        assert_eq!(map_sky("4"), "☁ 흐림");
        assert_eq!(map_sky("9"), "Unknown");
    }

    #[test]
    fn pty_mapping() {
        assert_eq!(map_pty("0"), "❌ 없음");
        assert_eq!(map_pty("1"), "🌧️ 비");
        assert_eq!(map_pty("3"), "🌨️ 눈");
        assert_eq!(map_pty("9"), "⚠ 오류");
    }

    #[test]
    fn base_time_picks_recent() {
        // 03:15 KST → 직전 발표는 02:00
        let kst = KST
            .with_ymd_and_hms(2024, 1, 1, 3, 15, 0)
            .unwrap()
            .with_timezone(&Utc);
        let (d, t) = compute_base_time(kst);
        assert_eq!(d, "20240101");
        assert_eq!(t, "0200");
    }

    #[test]
    fn base_time_before_0210_uses_yesterday_23() {
        // 01:30 KST → 어제 23:00 발표분
        let kst = KST
            .with_ymd_and_hms(2024, 1, 1, 1, 30, 0)
            .unwrap()
            .with_timezone(&Utc);
        let (d, t) = compute_base_time(kst);
        assert_eq!(d, "20231231");
        assert_eq!(t, "2300");
    }

    #[test]
    fn representative_slot_prefers_today_0900() {
        let today = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let items = vec![KmaItem {
            base_date: "20240101".into(),
            base_time: "0200".into(),
            category: "TMP".into(),
            fcst_date: "20240101".into(),
            fcst_time: "0900".into(),
            fcst_value: "3".into(),
            nx: 60,
            ny: 127,
        }];
        let s = pick_representative_slot(&items, today).unwrap();
        assert_eq!(s, ("20240101".to_string(), "0900".to_string()));
    }

    #[test]
    fn seoul_water_timestamp_parses_ymd_and_hr() {
        // 2024-03-01 15:00 KST = 2024-03-01 06:00 UTC
        let ts = parse_seoul_water_timestamp("2024-03-01", "15").unwrap();
        assert_eq!(
            ts,
            KST.with_ymd_and_hms(2024, 3, 1, 15, 0, 0)
                .unwrap()
                .with_timezone(&Utc)
        );
    }

    #[test]
    fn seoul_water_timestamp_rejects_bad_ymd() {
        let err = parse_seoul_water_timestamp("2024/03/01", "15").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("bad YMD/HR"), "unexpected error: {msg}");
    }

    #[test]
    fn seoul_water_timestamp_rejects_bad_hr() {
        let err = parse_seoul_water_timestamp("2024-03-01", "25").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("bad YMD/HR"), "unexpected error: {msg}");
    }
}
