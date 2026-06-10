//! 통합 에러 타입.
//!
//! [`HDMealError`] 는 도메인/인프라/핸들러 전 계층에서 공통으로 사용되며,
//! [`axum::response::IntoResponse`] 를 구현해 핸들러가 `Result<_, HDMealError>` 로
//! bubble up 하면 동일한 JSON envelope 으로 직렬화됩니다:
//!
//! ```json
//! { "detail": "<Korean message>", "requestId": "<uuidv7>" }
//! ```

use serde::Serialize;

/// Result alias.
pub type HDMealResult<T> = Result<T, HDMealError>;

/// 도메인 전반의 에러.
///
/// 각 variant 는 HTTP 상태 코드를 가집니다. 핸들러는 `Result<_, HDMealError>` 를
/// 반환하면 axum 의 `IntoResponse` 변환이 동일한 JSON envelope 으로 직렬화합니다.
#[derive(Debug, thiserror::Error)]
pub enum HDMealError {
    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),

    #[error("internal: {0}")]
    Internal(String),

    #[error("mongo: {0}")]
    Mongo(#[from] mongodb::error::Error),

    #[error("bson: {0}")]
    Bson(#[from] bson::error::Error),

    #[error("serde_json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("reqwest: {0}")]
    Http(#[from] reqwest::Error),

    #[error("jwt: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("url: {0}")]
    Url(#[from] url::ParseError),
}

impl HDMealError {
    pub fn bad_request<S: Into<String>>(msg: S) -> Self {
        Self::BadRequest(msg.into())
    }
    pub fn unauthorized<S: Into<String>>(msg: S) -> Self {
        Self::Unauthorized(msg.into())
    }
    pub fn forbidden<S: Into<String>>(msg: S) -> Self {
        Self::Forbidden(msg.into())
    }
    pub fn not_found<S: Into<String>>(msg: S) -> Self {
        Self::NotFound(msg.into())
    }
    pub fn service_unavailable<S: Into<String>>(msg: S) -> Self {
        Self::ServiceUnavailable(msg.into())
    }
    pub fn internal<S: Into<String>>(msg: S) -> Self {
        Self::Internal(msg.into())
    }

    pub fn status(&self) -> axum::http::StatusCode {
        use axum::http::StatusCode;
        match self {
            Self::BadRequest(_) | Self::Json(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized(_) | Self::Jwt(_) => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::ServiceUnavailable(_) | Self::Http(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Mongo(_) | Self::Bson(_) | Self::Internal(_) | Self::Io(_) | Self::Url(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    /// 사용자에게 노출되는 한국어 메시지. 내부 에러는 generic 한 문자열로 가립니다.
    pub fn public_message(&self) -> String {
        match self {
            Self::BadRequest(m) => m.clone(),
            Self::Json(_) => "잘못된 요청 본문입니다.".to_string(),
            Self::Unauthorized(m) => m.clone(),
            Self::Jwt(_) => "올바르지 않은 토큰입니다.".to_string(),
            Self::Forbidden(m) => m.clone(),
            Self::NotFound(m) => m.clone(),
            Self::ServiceUnavailable(m) => m.clone(),
            Self::Http(_) => "외부 API 연결에 실패했습니다.".to_string(),
            _ => "서버 오류가 발생했습니다".to_string(),
        }
    }
}

/// 응답 envelope. `requestId` 는 호출 시점에 채워서 넣습니다.
#[derive(Debug, Serialize)]
pub struct ErrorEnvelope {
    pub detail: String,
    #[serde(rename = "requestId")]
    pub request_id: String,
}

impl axum::response::IntoResponse for HDMealError {
    fn into_response(self) -> axum::response::Response {
        use axum::Json;

        let status = self.status();
        let request_id = crate::shared::context::current_request_id()
            .unwrap_or_else(crate::shared::context::new_request_id);
        let body = ErrorEnvelope {
            detail: self.public_message(),
            request_id: request_id.clone(),
        };
        let mut resp = (status, Json(body)).into_response();
        crate::transport::http::add_security_headers(resp.headers_mut());
        crate::shared::observability::write_request_id_headers(resp.headers_mut(), &request_id);
        resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn bad_request_status() {
        let e = HDMealError::bad_request("msg");
        assert_eq!(e.status(), StatusCode::BAD_REQUEST);
        assert_eq!(e.public_message(), "msg");
    }

    #[test]
    fn unauthorized_status() {
        let e = HDMealError::unauthorized("msg");
        assert_eq!(e.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(e.public_message(), "msg");
    }

    #[test]
    fn forbidden_status() {
        let e = HDMealError::forbidden("msg");
        assert_eq!(e.status(), StatusCode::FORBIDDEN);
        assert_eq!(e.public_message(), "msg");
    }

    #[test]
    fn not_found_status() {
        let e = HDMealError::not_found("msg");
        assert_eq!(e.status(), StatusCode::NOT_FOUND);
        assert_eq!(e.public_message(), "msg");
    }

    #[test]
    fn service_unavailable_status() {
        let e = HDMealError::service_unavailable("msg");
        assert_eq!(e.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(e.public_message(), "msg");
    }

    #[test]
    fn internal_status() {
        let e = HDMealError::internal("msg");
        assert_eq!(e.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(e.public_message(), "서버 오류가 발생했습니다");
    }

    #[test]
    fn json_error_maps_to_bad_request() {
        let json_err = serde_json::from_str::<i32>("not a number").unwrap_err();
        let e = HDMealError::Json(json_err);
        assert_eq!(e.status(), StatusCode::BAD_REQUEST);
        assert_eq!(e.public_message(), "잘못된 요청 본문입니다.");
    }

    #[test]
    fn jwt_error_maps_to_unauthorized() {
        let jwt_err = jsonwebtoken::decode_header("not-a-jwt").unwrap_err();
        let e = HDMealError::Jwt(jwt_err);
        assert_eq!(e.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(e.public_message(), "올바르지 않은 토큰입니다.");
    }

    #[test]
    fn http_error_maps_to_service_unavailable() {
        use tokio::runtime::Runtime;
        crate::shared::tls::install_rustls_ring_provider();

        let rt = Runtime::new().unwrap();
        let err = rt.block_on(async {
            let client = reqwest::Client::builder().build().unwrap();
            let url = reqwest::Url::parse("http://0.0.0.0:1").unwrap();
            let req = client.get(url).build().unwrap();
            client.execute(req).await.unwrap_err()
        });
        let e = HDMealError::Http(err);
        assert_eq!(e.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(e.public_message(), "외부 API 연결에 실패했습니다.");
    }

    #[test]
    fn io_error_maps_to_internal() {
        let e = HDMealError::Io(std::io::Error::other("io fail"));
        assert_eq!(e.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(e.public_message(), "서버 오류가 발생했습니다");
    }
}
