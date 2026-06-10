//! 인증/인가 도구.
//!
//! - [`authorize_skill_token`] : `/skill/` 의 `HDMeal_AuthTokens` (HS 토큰 목록)
//!   를 상수 시간 비교.
//! - [`issue_user_token`] / [`validate_user_token`] : HS256 JWT, issuer =
//!   `"HDMeal-UserSettings"`, TTL 10분.

use chrono::{Duration, Utc};
use jsonwebtoken::crypto::{CryptoProvider, JwkUtils, JwtSigner, JwtVerifier};
use jsonwebtoken::errors::{ErrorKind, Result as JwtResult};
use jsonwebtoken::signature::{Signer, Verifier};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use ring::hmac;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;
use subtle::ConstantTimeEq;

use crate::error::{HDMealError, HDMealResult};

type HmacSha256 = hmac::Key;

static JWT_HMAC_PROVIDER: CryptoProvider = CryptoProvider {
    signer_factory: jwt_hmac_signer_factory,
    verifier_factory: jwt_hmac_verifier_factory,
    jwk_utils: JwkUtils::new_unimplemented(),
};

static JWT_HMAC_PROVIDER_INIT: OnceLock<()> = OnceLock::new();

/// jsonwebtoken 의 process-wide crypto provider 를 HS256 전용 HMAC 구현으로 설치한다.
///
/// 앱 초기화 경로와 JWT helper 들에서 호출해도 안전하도록 한 번만 설치한다.
pub fn install_jwt_hmac_provider() {
    JWT_HMAC_PROVIDER_INIT.get_or_init(|| {
        let _ = JWT_HMAC_PROVIDER.install_default();
    });
}

fn jwt_hmac_signer_factory(
    algorithm: &Algorithm,
    encoding_key: &EncodingKey,
) -> JwtResult<Box<dyn JwtSigner>> {
    match algorithm {
        Algorithm::HS256 => Ok(Box::new(Hs256Signer::new(encoding_key)?) as Box<dyn JwtSigner>),
        _ => Err(ErrorKind::InvalidAlgorithm.into()),
    }
}

fn jwt_hmac_verifier_factory(
    algorithm: &Algorithm,
    decoding_key: &DecodingKey,
) -> JwtResult<Box<dyn JwtVerifier>> {
    match algorithm {
        Algorithm::HS256 => Ok(Box::new(Hs256Verifier::new(decoding_key)?) as Box<dyn JwtVerifier>),
        _ => Err(ErrorKind::InvalidAlgorithm.into()),
    }
}

struct Hs256Signer(HmacSha256);

impl Hs256Signer {
    fn new(encoding_key: &EncodingKey) -> JwtResult<Self> {
        let inner = HmacSha256::new(hmac::HMAC_SHA256, encoding_key.try_get_hmac_secret()?);
        Ok(Self(inner))
    }
}

impl Signer<Vec<u8>> for Hs256Signer {
    fn try_sign(&self, msg: &[u8]) -> std::result::Result<Vec<u8>, jsonwebtoken::signature::Error> {
        Ok(hmac::sign(&self.0, msg).as_ref().to_vec())
    }
}

impl JwtSigner for Hs256Signer {
    fn algorithm(&self) -> Algorithm {
        Algorithm::HS256
    }
}

struct Hs256Verifier(HmacSha256);

impl Hs256Verifier {
    fn new(decoding_key: &DecodingKey) -> JwtResult<Self> {
        let inner = HmacSha256::new(hmac::HMAC_SHA256, decoding_key.try_get_hmac_secret()?);
        Ok(Self(inner))
    }
}

impl Verifier<Vec<u8>> for Hs256Verifier {
    fn verify(
        &self,
        msg: &[u8],
        signature: &Vec<u8>,
    ) -> std::result::Result<(), jsonwebtoken::signature::Error> {
        hmac::verify(&self.0, msg, signature.as_slice())
            .map_err(|_| jsonwebtoken::signature::Error::new())
    }
}

impl JwtVerifier for Hs256Verifier {
    fn algorithm(&self) -> Algorithm {
        Algorithm::HS256
    }
}

/// `/skill/` 인증 토큰. 상수 시간 비교로 안전하게 검증한다.
///
/// 비교는 SHA-256 digest 로 고정 길이화한 뒤 진행한다. `iter().any`
/// 같은 조기 반환 경로를 없애고, allowed 목록의 모든 항목을 끝까지 확인한다.
pub(crate) fn hash_skill_token(token: &str) -> [u8; 32] {
    let digest = Sha256::digest(token.as_bytes());
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    out
}

/// 이미 해시된 `/skill/` 토큰 목록과 상수 시간 비교.
pub(crate) fn authorize_skill_token_hashed(
    provided: Option<&str>,
    allowed_hashes: &[[u8; 32]],
) -> bool {
    let Some(provided) = provided else {
        return false;
    };
    let provided_hash = Sha256::digest(provided.as_bytes());
    let provided_hash: &[u8] = provided_hash.as_ref();
    let mut any_match = 0u8;
    for allowed_hash in allowed_hashes {
        let allowed_hash: &[u8] = allowed_hash.as_ref();
        any_match |= provided_hash.ct_eq(allowed_hash).unwrap_u8();
    }
    any_match != 0
}

/// `/skill/` 인증 토큰. 상수 시간 비교로 안전하게 검증한다.
///
/// 테스트/호출 편의용 raw-string 경로. production 은 pre-hash 경로를 사용한다.
pub fn authorize_skill_token(provided: Option<&str>, allowed: &[String]) -> bool {
    let allowed_hashes: Vec<[u8; 32]> = allowed
        .iter()
        .map(|token| hash_skill_token(token))
        .collect();
    authorize_skill_token_hashed(provided, &allowed_hashes)
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
///
/// # Errors
///
/// - `HDMealError::Internal` — `secret` 가 빈 문자열인 경우.
/// - `jsonwebtoken::Error` — 인코딩 실패 (극히 드묾).
pub fn issue_user_token(input: IssueUserTokenInput<'_>) -> HDMealResult<String> {
    install_jwt_hmac_provider();

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
///
/// # Errors
///
/// - `HDMealError::Unauthorized` — 빈 secret, 서명 불일치, 만료, `sub != uid`.
/// - `HDMealError::Forbidden` — scope 부족.
pub fn validate_user_token(input: ValidateUserTokenInput<'_>) -> HDMealResult<UserTokenClaims> {
    install_jwt_hmac_provider();

    if input.secret.trim().is_empty() {
        return Err(HDMealError::unauthorized("올바르지 않은 토큰입니다."));
    }

    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_issuer(&[JWT_ISSUER]);
    validation.set_required_spec_claims(&["iss", "uid", "scope", "reqId", "nbf", "exp"]);
    validation.validate_nbf = true;
    // 분산 환경에서 클럭 스큐를 수용하기 위해 30초 leeway 허용.
    validation.leeway = 30;
    let data = decode::<UserTokenClaims>(
        input.token,
        &DecodingKey::from_secret(input.secret.as_bytes()),
        &validation,
    )
    .map_err(|_| HDMealError::unauthorized("올바르지 않은 토큰입니다."))?;

    let claims = data.claims;
    if claims.sub != claims.uid {
        return Err(HDMealError::unauthorized("올바르지 않은 토큰입니다."));
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
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

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
        install_jwt_hmac_provider();

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
        install_jwt_hmac_provider();

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
        install_jwt_hmac_provider();

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
        assert_eq!(err.status(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn jwt_accepts_nbf_within_leeway() {
        install_jwt_hmac_provider();

        let secret = "test-secret";
        let now = Utc::now().timestamp();
        let claims = UserTokenClaims {
            iss: JWT_ISSUER.to_string(),
            sub: "KT:abc".to_string(),
            jti: crate::shared::context::new_request_id(),
            iat: now,
            nbf: now + 20,
            exp: now + 600,
            uid: "KT:abc".to_string(),
            scope: vec![scope::GET_USER_INFO.to_string()],
            req_id: crate::shared::context::new_request_id(),
        };
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();
        let validated = validate_user_token(ValidateUserTokenInput {
            token: &token,
            secret,
            required_scope: scope::GET_USER_INFO,
        })
        .unwrap();
        assert_eq!(validated.uid, "KT:abc");
    }

    #[test]
    fn jwt_rejects_nbf_beyond_leeway() {
        install_jwt_hmac_provider();

        let secret = "test-secret";
        let now = Utc::now().timestamp();
        let claims = UserTokenClaims {
            iss: JWT_ISSUER.to_string(),
            sub: "KT:abc".to_string(),
            jti: crate::shared::context::new_request_id(),
            iat: now,
            nbf: now + 40,
            exp: now + 600,
            uid: "KT:abc".to_string(),
            scope: vec![scope::GET_USER_INFO.to_string()],
            req_id: crate::shared::context::new_request_id(),
        };
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();
        let err = validate_user_token(ValidateUserTokenInput {
            token: &token,
            secret,
            required_scope: scope::GET_USER_INFO,
        })
        .unwrap_err();
        assert_eq!(err.status(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn split_uid_validates() {
        assert_eq!(split_uid("KT:abc").unwrap(), ("KT", "abc"));
        assert!(split_uid("KT:").is_err());
        assert!(split_uid(":abc").is_err());
        assert!(split_uid("KT").is_err());
    }
}
