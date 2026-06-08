//! 캐시 freshness 유틸. `created_at` 기준 TTL 만료 여부 판정. 모든 계층에서 사용 가능.

use std::time::Duration;

use chrono::{DateTime, Utc};

/// `created_at` 이 `now` 기준으로 TTL 이내면 `true`.
pub fn is_fresh(created_at: DateTime<Utc>, ttl: Duration) -> bool {
    let now = Utc::now();
    let age = now.signed_duration_since(created_at);
    age <= chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::seconds(0))
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
