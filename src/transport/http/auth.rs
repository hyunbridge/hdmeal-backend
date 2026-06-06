//! HTTP 인증 토큰 추출 (skill / user settings 공통).

use std::collections::HashMap;

use axum::http::HeaderMap;

/// 통합 토큰 추출: 모든 인증 엔드포인트에서 동일한 우선순위로 토큰을 찾는다.
///
/// 우선순위 (보안상 안전한 순서):
///   1. `X-HDMeal-Token` 헤더
///   2. `Authorization: Bearer <token>` 헤더
///   3. `?token=` 쿼리 (debug 모드에서만 허용)
pub fn extract_token(
    headers: &HeaderMap,
    query: &HashMap<String, String>,
    allow_query_token: bool,
) -> Option<String> {
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| {
            let (scheme, token) = s.trim().split_once(' ')?;
            if scheme.eq_ignore_ascii_case("bearer") {
                Some(token.trim_matches(' '))
            } else {
                None
            }
        });
    let query_token = allow_query_token
        .then(|| query.get("token").map(String::as_str))
        .flatten();
    let candidates: [Option<&str>; 3] = [
        headers.get("X-HDMeal-Token").and_then(|v| v.to_str().ok()),
        bearer,
        query_token,
    ];
    candidates
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|s| !s.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue};

    fn headers_with(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    fn empty_query() -> HashMap<String, String> {
        HashMap::new()
    }

    #[test]
    fn extract_token_prefers_x_hdmeal_token_header() {
        let h = headers_with(&[("X-HDMeal-Token", "alpha")]);
        let q = empty_query();
        assert_eq!(extract_token(&h, &q, false), Some("alpha".to_string()));
    }

    #[test]
    fn extract_token_falls_back_to_bearer_header() {
        let h = headers_with(&[("authorization", "Bearer beta")]);
        let q = empty_query();
        assert_eq!(extract_token(&h, &q, false), Some("beta".to_string()));
    }

    #[test]
    fn extract_token_accepts_trimmed_lowercase_bearer_header() {
        let h = headers_with(&[("authorization", "  bEaReR   beta  ")]);
        let q = empty_query();
        assert_eq!(extract_token(&h, &q, false), Some("beta".to_string()));
    }

    #[test]
    fn extract_token_rejects_query_by_default() {
        let h = headers_with(&[]);
        let mut q = empty_query();
        q.insert("token".to_string(), "gamma".to_string());
        assert_eq!(extract_token(&h, &q, false), None);
    }

    #[test]
    fn extract_token_allows_query_when_enabled() {
        let h = headers_with(&[]);
        let mut q = empty_query();
        q.insert("token".to_string(), "gamma".to_string());
        assert_eq!(extract_token(&h, &q, true), Some("gamma".to_string()));
    }

    #[test]
    fn extract_token_priority_header_over_query() {
        let h = headers_with(&[("X-HDMeal-Token", "alpha")]);
        let mut q = empty_query();
        q.insert("token".to_string(), "gamma".to_string());
        assert_eq!(extract_token(&h, &q, true), Some("alpha".to_string()));
    }

    #[test]
    fn extract_token_priority_bearer_over_query() {
        let h = headers_with(&[("authorization", "Bearer beta")]);
        let mut q = empty_query();
        q.insert("token".to_string(), "gamma".to_string());
        assert_eq!(extract_token(&h, &q, true), Some("beta".to_string()));
    }

    #[test]
    fn extract_token_trims_whitespace() {
        let h = headers_with(&[("X-HDMeal-Token", "  alpha  ")]);
        let q = empty_query();
        assert_eq!(extract_token(&h, &q, false), Some("alpha".to_string()));
    }

    #[test]
    fn extract_token_rejects_empty() {
        let h = headers_with(&[("X-HDMeal-Token", "   ")]);
        let q = empty_query();
        assert_eq!(extract_token(&h, &q, false), None);
    }

    #[test]
    fn extract_token_rejects_bearer_without_prefix() {
        let h = headers_with(&[("authorization", "Basic dXNlcjpwYXNz")]);
        let q = empty_query();
        assert_eq!(extract_token(&h, &q, false), None);
    }

    #[test]
    fn extract_token_rejects_tab_separated_bearer() {
        let h = headers_with(&[("authorization", "Bearer\tbeta")]);
        let q = empty_query();
        assert_eq!(extract_token(&h, &q, false), None);
    }

    #[test]
    fn extract_token_returns_none_when_all_empty() {
        let h = headers_with(&[]);
        let q = empty_query();
        assert_eq!(extract_token(&h, &q, false), None);
    }
}
