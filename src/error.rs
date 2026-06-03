//! 통합 에러 타입.
//!
//! [`HDMealError`] 는 도메인/인프라/핸들러 전 계층에서 공통으로 사용되며,
//! [`warp::reject::Reject`] 를 구현해 Warp 의 `with_recover` 패턴과 자연스럽게
//! 결합됩니다:
//!
//! ```json
//! { "detail": "<Korean message>", "requestId": "<uuidv7>" }
//! ```

use std::convert::Infallible;

use serde::Serialize;
use warp::http::StatusCode;
use warp::reject::Reject;
use warp::reply::Reply;

/// Result alias.
pub type HDMealResult<T> = Result<T, HDMealError>;

/// 도메인 전반의 에러.
///
/// 각 variant 는 HTTP 상태 코드를 가집니다. `Into<warp::reply::Response>` 가
/// [`HDMealError::into_reply`] 를 통해 동일한 JSON envelope 으로 직렬화합니다.
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

    #[error("bson serialize: {0}")]
    BsonSer(#[from] bson::ser::Error),

    #[error("bson deserialize: {0}")]
    BsonDe(#[from] bson::de::Error),

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

    pub fn status(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) | Self::Json(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized(_) | Self::Jwt(_) => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::ServiceUnavailable(_) | Self::Http(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Mongo(_)
            | Self::BsonSer(_)
            | Self::BsonDe(_)
            | Self::Internal(_)
            | Self::Io(_)
            | Self::Url(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// 사용자에게 노출되는 한국어 메시지. 내부 에러는 generic 한 문자열로 가립니다.
    pub fn public_message(&self) -> String {
        match self {
            Self::BadRequest(m) => m.clone(),
            Self::Json(e) => format!("잘못된 요청 본문입니다: {e}"),
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

impl Reject for HDMealError {}

/// 응답 envelope. `requestId` 는 호출 시점에 채워서 넣습니다.
#[derive(Debug, Serialize)]
pub struct ErrorEnvelope {
    pub detail: String,
    #[serde(rename = "requestId")]
    pub request_id: String,
}

/// `HDMealError` → `warp::reply::Response` 변환. 핸들러는 그냥 `?` 로 bubble up 하고
/// `with_recover` 가 이 함수를 호출합니다.
pub async fn handle_rejection(err: warp::Rejection) -> Result<warp::reply::Response, Infallible> {
    let (status, detail) = if let Some(e) = err.find::<HDMealError>() {
        (e.status(), e.public_message())
    } else if err.is_not_found() {
        (
            StatusCode::NOT_FOUND,
            "요청한 경로를 찾을 수 없습니다.".to_string(),
        )
    } else if err.find::<warp::reject::MethodNotAllowed>().is_some() {
        (
            StatusCode::METHOD_NOT_ALLOWED,
            "허용되지 않은 메서드입니다.".to_string(),
        )
    } else if err.find::<warp::reject::InvalidQuery>().is_some() {
        (StatusCode::BAD_REQUEST, "잘못된 쿼리입니다.".to_string())
    } else if err.find::<warp::body::BodyDeserializeError>().is_some() {
        (
            StatusCode::BAD_REQUEST,
            "잘못된 요청 본문입니다.".to_string(),
        )
    } else if err.find::<warp::reject::MissingHeader>().is_some() {
        (StatusCode::BAD_REQUEST, "필수 헤더가 없습니다.".to_string())
    } else if err.find::<warp::reject::PayloadTooLarge>().is_some() {
        (
            StatusCode::PAYLOAD_TOO_LARGE,
            "요청 본문이 너무 큽니다.".to_string(),
        )
    } else {
        tracing::error!(error = ?err, "unhandled rejection");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "서버 오류가 발생했습니다".to_string(),
        )
    };

    let request_id = crate::shared::context::current_request_id()
        .unwrap_or_else(crate::shared::context::new_request_id);

    let body = ErrorEnvelope { detail, request_id };
    let mut resp = warp::reply::with_status(warp::reply::json(&body), status).into_response();
    crate::transport::http::add_security_headers(resp.headers_mut());
    crate::shared::observability::write_request_id_headers(resp.headers_mut(), &body.request_id);
    Ok(resp)
}

/// 500 응답을 명시적으로 만들고 싶을 때.
pub fn internal_500<S: std::fmt::Display>(msg: S) -> warp::reply::Response {
    let body = ErrorEnvelope {
        detail: format!("서버 오류가 발생했습니다: {msg}"),
        request_id: crate::shared::context::current_request_id()
            .unwrap_or_else(crate::shared::context::new_request_id),
    };
    let mut resp =
        warp::reply::with_status(warp::reply::json(&body), StatusCode::INTERNAL_SERVER_ERROR)
            .into_response();
    crate::transport::http::add_security_headers(resp.headers_mut());
    crate::shared::observability::write_request_id_headers(resp.headers_mut(), &body.request_id);
    resp
}
