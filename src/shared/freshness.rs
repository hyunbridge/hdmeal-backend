//! 캐시 freshness 유틸. `created_at` 기준 TTL 만료 여부 판정. 모든 계층에서 사용 가능.

use std::time::Duration;

use chrono::{DateTime, Utc};

/// `created_at` 이 `now` 기준으로 TTL 이내면 `true`.
pub fn is_fresh(created_at: DateTime<Utc>, ttl: Duration) -> bool {
    is_fresh_at(created_at, ttl, Utc::now())
}

/// [`is_fresh`] 의 결정론적 변형. 테스트에서 `now` 를 주입할 수 있다.
pub fn is_fresh_at(created_at: DateTime<Utc>, ttl: Duration, now: DateTime<Utc>) -> bool {
    let age = now.signed_duration_since(created_at);
    age <= chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::seconds(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn fresh_within_ttl() {
        assert!(is_fresh(
            Utc::now() - chrono::Duration::seconds(5),
            Duration::from_secs(60),
        ));
    }

    #[test]
    fn stale_beyond_ttl() {
        assert!(!is_fresh(
            Utc::now() - chrono::Duration::seconds(120),
            Duration::from_secs(60),
        ));
    }

    #[test]
    fn is_fresh_at_exact_boundary() {
        let now = Utc.with_ymd_and_hms(2026, 6, 9, 12, 0, 0).unwrap();
        let created = now - chrono::Duration::seconds(60);
        let ttl = Duration::from_secs(60);
        assert!(is_fresh_at(created, ttl, now));
    }

    #[test]
    fn is_fresh_at_one_second_past_ttl() {
        let now = Utc.with_ymd_and_hms(2026, 6, 9, 12, 0, 0).unwrap();
        let created = now - chrono::Duration::seconds(61);
        let ttl = Duration::from_secs(60);
        assert!(!is_fresh_at(created, ttl, now));
    }

    #[test]
    fn is_fresh_at_zero_ttl_only_exact_match() {
        let now = Utc.with_ymd_and_hms(2026, 6, 9, 12, 0, 0).unwrap();
        assert!(is_fresh_at(now, Duration::from_secs(0), now));
        assert!(!is_fresh_at(
            now - chrono::Duration::seconds(1),
            Duration::from_secs(0),
            now
        ));
    }
}
