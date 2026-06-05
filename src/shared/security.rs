//! 인증/인가 도구.
//!
//! - [`authorize_skill_token`] : `/skill/` 의 `HDMeal_AuthTokens` (HS 토큰 목록)
//!   를 상수 시간 비교.
//! - [`issue_user_token`] / [`validate_user_token`] : HS256 JWT, issuer =
//!   `"HDMeal-UserSettings"`, TTL 10분.

use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::error::{HDMealError, HDMealResult};

/// `/skill/` 인증 토큰. 상수 시간 비교로 안전하게 검증한다.
pub fn authorize_skill_token(provided: Option<&str>, allowed: &[String]) -> bool {
    let Some(provided) = provided else {
        return false;
    };
    let p = provided.as_bytes();
    allowed.iter().any(|a| a.as_bytes().ct_eq(p).into())
}

/// user-settings JWT 발급자. issuer = `"HDMeal-UserSettings"`, TTL 10분.
pub const JWT_ISSUER: &str = "HDMeal-UserSettings";

/// user-settings JWT scope.
pub mod scope {
    pub const GET_USER_INFO: &str = "GetUserInfo";
    pub const MANAGE_USER_INFO: &str = "ManageUserInfo";
    pub const GET_USAGE_DATA: &str = "GetUsageData";
    pub const DELETE_USAGE_DATA: &str = "DeleteUsageData";
}

/// JWT 클레임. `sub == uid` invariant 는 [`validate_user_token`] 에서 강제.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserTokenClaims {
    pub iss: String,
    pub sub: String,
    pub jti: String,
    pub iat: i64,
    pub nbf: i64,
    pub exp: i64,
    pub uid: String,
    pub scope: Vec<String>,
    #[serde(rename = "reqId")]
    pub req_id: String,
}

#[derive(Debug, Clone)]
pub struct IssueUserTokenInput<'a> {
    pub secret: &'a str,
    pub uid: &'a str,
    pub scope: Vec<String>,
    pub req_id: &'a str,
    pub ttl: Duration,
}

/// 10분짜리 user-settings JWT 를 발급합니다.
pub fn issue_user_token(input: IssueUserTokenInput<'_>) -> HDMealResult<String> {
    if input.secret.is_empty() {
        return Err(HDMealError::internal("JWT secret is empty"));
    }
    let now = Utc::now();
    let uid = input.uid.to_owned();
    let claims = UserTokenClaims {
        iss: JWT_ISSUER.to_string(),
        sub: uid.clone(),
        jti: crate::shared::context::new_request_id(),
        iat: now.timestamp(),
        nbf: now.timestamp(),
        exp: (now + input.ttl).timestamp(),
        uid,
        scope: input.scope,
        req_id: input.req_id.to_string(),
    };
    let token = encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(input.secret.as_bytes()),
    )?;
    Ok(token)
}

#[derive(Debug, Clone)]
pub struct ValidateUserTokenInput<'a> {
    pub token: &'a str,
    pub secret: &'a str,
    pub required_scope: &'a str,
}

/// JWT 를 검증하고 클레임을 반환합니다.
pub fn validate_user_token(input: ValidateUserTokenInput<'_>) -> HDMealResult<UserTokenClaims> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_issuer(&[JWT_ISSUER]);
    validation.set_required_spec_claims(&["iss", "uid", "scope", "reqId", "nbf", "exp"]);
    let data = decode::<UserTokenClaims>(
        input.token,
        &DecodingKey::from_secret(input.secret.as_bytes()),
        &validation,
    )
    .map_err(|_| HDMealError::forbidden("올바르지 않은 토큰입니다."))?;

    let claims = data.claims;
    if claims.sub != claims.uid {
        return Err(HDMealError::forbidden("올바르지 않은 토큰입니다."));
    }
    if !claims.scope.iter().any(|s| s == input.required_scope) {
        return Err(HDMealError::forbidden("권한이 없습니다."));
    }
    Ok(claims)
}

/// `uid` 가 `"platform:external_id"` 형식인지 검증. 빈 부분이 있으면 에러.
pub fn split_uid(uid: &str) -> HDMealResult<(&str, &str)> {
    match uid.split_once(':') {
        Some((platform, external)) if !platform.is_empty() && !external.is_empty() => {
            Ok((platform, external))
        }
        _ => Err(HDMealError::bad_request("올바르지 않은 토큰입니다.")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_token_constant_time() {
        let allowed = vec!["alpha".to_string(), "beta".to_string()];
        assert!(authorize_skill_token(Some("alpha"), &allowed));
        assert!(authorize_skill_token(Some("beta"), &allowed));
        assert!(!authorize_skill_token(Some("gamma"), &allowed));
        assert!(!authorize_skill_token(None, &allowed));
        assert!(!authorize_skill_token(Some(""), &allowed));
    }

    #[test]
    fn jwt_round_trip() {
        let secret = "test-secret";
        let req_id = crate::shared::context::new_request_id();
        let token = issue_user_token(IssueUserTokenInput {
            secret,
            uid: "KT:abc",
            scope: vec![scope::GET_USER_INFO.into(), scope::MANAGE_USER_INFO.into()],
            req_id: &req_id,
            ttl: Duration::minutes(10),
        })
        .unwrap();
        let claims = validate_user_token(ValidateUserTokenInput {
            token: &token,
            secret,
            required_scope: scope::GET_USER_INFO,
        })
        .unwrap();
        assert_eq!(claims.uid, "KT:abc");
        assert_eq!(claims.req_id, req_id);
        assert_eq!(claims.scope.len(), 2);
    }

    #[test]
    fn jwt_rejects_wrong_scope() {
        let secret = "test-secret";
        let req_id = crate::shared::context::new_request_id();
        let token = issue_user_token(IssueUserTokenInput {
            secret,
            uid: "KT:abc",
            scope: vec![scope::GET_USER_INFO.into()],
            req_id: &req_id,
            ttl: Duration::minutes(10),
        })
        .unwrap();
        let err = validate_user_token(ValidateUserTokenInput {
            token: &token,
            secret,
            required_scope: scope::MANAGE_USER_INFO,
        })
        .unwrap_err();
        assert_eq!(err.status(), axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn jwt_rejects_bad_signature() {
        let req_id = crate::shared::context::new_request_id();
        let token = issue_user_token(IssueUserTokenInput {
            secret: "secret-1",
            uid: "KT:abc",
            scope: vec![scope::GET_USER_INFO.into()],
            req_id: &req_id,
            ttl: Duration::minutes(10),
        })
        .unwrap();
        let err = validate_user_token(ValidateUserTokenInput {
            token: &token,
            secret: "secret-2",
            required_scope: scope::GET_USER_INFO,
        })
        .unwrap_err();
        assert_eq!(err.status(), axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn split_uid_validates() {
        assert_eq!(split_uid("KT:abc").unwrap(), ("KT", "abc"));
        assert!(split_uid("KT:").is_err());
        assert!(split_uid(":abc").is_err());
        assert!(split_uid("KT").is_err());
    }
}
