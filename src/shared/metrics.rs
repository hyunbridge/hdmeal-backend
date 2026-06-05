//! 최소 메트릭 — Prometheus text format 으로 노출.
//!
//! 범위:
//! - `http_requests_total{path,method,status}` 카운터
//! - `process_start_time_seconds` 게이지
//!
//! 의도적으로 OTel metrics SDK 를 도입하지 않고 [`prometheus` 크레이트도
//! 쓰지 않습니다 — 현재 코드 베이스의 모든 카운터는 단순한 hashmap +
//! atomic 으로 충분합니다. 분산 트레이싱은 이미 OTel Tracer 로 커버됩니다.
//! 추후 라벨 카디널리티 / 히스토그램이 필요해지면 `prometheus` 크레이트로
//! 교체하는 게 가장 적은 비용입니다.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use std::sync::Mutex;

/// 프로세스 시작 시각 (epoch seconds). 게이지 한 줄로 노출.
pub struct Metrics {
    start_time_secs: AtomicU64,
    /// (path, method, status) -> count. 정렬된 직렬화를 위해 BTreeMap.
    requests: Mutex<BTreeMap<RequestKey, u64>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RequestKey {
    path: String,
    method: String,
    status: u16,
}

impl Metrics {
    pub fn new() -> Self {
        let start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self {
            start_time_secs: AtomicU64::new(start),
            requests: Mutex::new(BTreeMap::new()),
        }
    }

    /// HTTP 요청 1 건을 카운트.
    pub fn record_request(&self, path: &str, method: &str, status: u16) {
        let mut map = self.requests.lock().unwrap();
        let entry = map
            .entry(RequestKey {
                path: path.to_owned(),
                method: method.to_owned(),
                status,
            })
            .or_insert(0);
        *entry += 1;
    }

    /// Prometheus text format 직렬화.
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(1024);
        out.push_str("# HELP http_requests_total Total HTTP requests by path, method, status.\n");
        out.push_str("# TYPE http_requests_total counter\n");

        let map = self.requests.lock().unwrap();
        for (key, count) in map.iter() {
            let _ = writeln!(
                out,
                "http_requests_total{{path=\"{}\",method=\"{}\",status=\"{}\"}} {}",
                escape_label(&key.path),
                escape_label(&key.method),
                key.status,
                count,
            );
        }
        drop(map);

        out.push_str(
            "# HELP process_start_time_seconds Unix epoch seconds when the process started.\n",
        );
        out.push_str("# TYPE process_start_time_seconds gauge\n");
        let _ = writeln!(
            out,
            "process_start_time_seconds {}",
            self.start_time_secs.load(Ordering::Relaxed),
        );
        out
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

fn escape_label(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            _ => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_render_has_help_lines() {
        let m = Metrics::new();
        let r = m.render();
        assert!(r.contains("# HELP http_requests_total"));
        assert!(r.contains("# TYPE http_requests_total counter"));
        assert!(r.contains("process_start_time_seconds"));
    }

    #[test]
    fn record_increments_counter() {
        let m = Metrics::new();
        m.record_request("/healthz", "GET", 200);
        m.record_request("/healthz", "GET", 200);
        m.record_request("/healthz", "GET", 500);
        m.record_request("/api/app/days", "GET", 200);

        let r = m.render();
        assert!(
            r.contains("http_requests_total{path=\"/healthz\",method=\"GET\",status=\"200\"} 2")
        );
        assert!(
            r.contains("http_requests_total{path=\"/healthz\",method=\"GET\",status=\"500\"} 1")
        );
        assert!(r.contains(
            "http_requests_total{path=\"/api/app/days\",method=\"GET\",status=\"200\"} 1"
        ));
    }

    #[test]
    fn escapes_quotes_and_backslashes() {
        let m = Metrics::new();
        m.record_request("/x\"y", "GET", 200);
        m.record_request("/x\\y", "GET", 200);
        let r = m.render();
        assert!(r.contains(r#"path="/x\"y""#));
        assert!(r.contains(r#"path="/x\\y""#));
    }
}
