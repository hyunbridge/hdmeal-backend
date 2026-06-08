//! 요청 컨텍스트: UUIDv7 기반 request ID.
//!
//! - [`new_request_id`] 는 항상 새 UUIDv7 을 발급합니다.
//! - [`normalize_request_id`] 는 입력 문자열이 UUIDv7 일 때만 그대로 사용하고,
//!   그 외에는 `None` 을 반환합니다.
//! - [`current_request_id`] / [`scope_request_id`] 는 `tokio::task_local!` 로
//!   현재 비동기 작업의 request ID 를 보관합니다. 응답 envelope, 로그, OTel
//!   span attribute 에 같은 값을 채우기 위한 단일 출처입니다.

use std::future::Future;

use tokio::task_local;
use uuid::Uuid;

task_local! {
    /// 현재 비동기 작업의 request ID.
    pub static REQUEST_ID: String;
}

/// 새 UUIDv7 을 발급합니다.
pub fn new_request_id() -> String {
    Uuid::now_v7().to_string()
}

/// 입력 문자열이 UUIDv7 이면 `Some(원본)`, 아니면 `None`.
pub fn normalize_request_id<S: AsRef<str>>(raw: S) -> Option<String> {
    let s = raw.as_ref();
    Uuid::parse_str(s).ok().and_then(|u| {
        // v7 만 허용. get_version() 은 u8 반환.
        if u.get_version_num() == 7 {
            Some(s.to_string())
        } else {
            None
        }
    })
}

/// 현재 작업의 request ID 를 반환합니다. 없으면 `None`.
pub fn current_request_id() -> Option<String> {
    REQUEST_ID.try_with(|id| id.clone()).ok()
}

/// 주어진 request ID 로 `fut` 를 실행합니다. 내부에서 에러 envelope / 로그가
/// 동일한 ID 를 볼 수 있도록 합니다.
pub async fn scope_request_id<F, T>(id: String, fut: F) -> T
where
    F: Future<Output = T>,
{
    REQUEST_ID.scope(id, fut).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_v7() {
        let id = new_request_id();
        let parsed = Uuid::parse_str(&id).unwrap();
        assert_eq!(parsed.get_version_num(), 7);
    }

    #[test]
    fn normalize_accepts_v7() {
        let id = new_request_id();
        assert_eq!(normalize_request_id(&id).as_deref(), Some(id.as_str()));
    }

    #[test]
    fn normalize_rejects_v4_and_garbage() {
        let v4 = Uuid::new_v4().to_string();
        assert!(normalize_request_id(&v4).is_none());
        assert!(normalize_request_id("not-a-uuid").is_none());
        assert!(normalize_request_id("").is_none());
    }

    #[tokio::test]
    async fn scope_propagates() {
        let id = new_request_id();
        let seen = scope_request_id(id.clone(), async { current_request_id() }).await;
        assert_eq!(seen, Some(id));
    }
}
