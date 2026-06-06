//! HTTP 핸들러 ↔ 직렬화/역직렬화 통합 테스트.

use hdmeal_backend::application::chatbot::types::{IntentKind, KakaoAction, KakaoSkillRequest};
use hdmeal_backend::error::HDMealError;
use hdmeal_backend::shared::security::{scope, IssueUserTokenInput, ValidateUserTokenInput};
use hdmeal_backend::shared::timezone::{
    format_date_label, format_hour, kst_midnight_to_utc, today_kst_date,
};
use hdmeal_backend::transport::http::dto::user_settings::UpdateUserSettingsRequest;
use std::collections::HashMap;

use chrono::Duration as ChronoDuration;

#[test]
fn error_envelope_shape() {
    let e = HDMealError::bad_request("잘바르지 않은 요청입니다.");
    assert_eq!(e.status(), axum::http::StatusCode::BAD_REQUEST);
    assert_eq!(e.public_message(), "잘바르지 않은 요청입니다.");
}

#[test]
fn error_unauthorized() {
    let e = HDMealError::unauthorized("토큰이 없습니다.");
    assert_eq!(e.status(), axum::http::StatusCode::UNAUTHORIZED);
    assert_eq!(e.public_message(), "토큰이 없습니다.");
}

#[test]
fn jwt_full_workflow() {
    let secret = "integration-secret";
    let req_id = hdmeal_backend::shared::context::new_request_id();
    let token = hdmeal_backend::shared::security::issue_user_token(IssueUserTokenInput {
        secret,
        uid: "KT:test-user",
        scope: vec![scope::GET_USER_INFO.into(), scope::MANAGE_USER_INFO.into()],
        req_id: &req_id,
        ttl: chrono::Duration::minutes(10),
    })
    .unwrap();

    let claims_get =
        hdmeal_backend::shared::security::validate_user_token(ValidateUserTokenInput {
            token: &token,
            secret,
            required_scope: scope::GET_USER_INFO,
        })
        .unwrap();
    assert_eq!(claims_get.uid, "KT:test-user");
    assert_eq!(claims_get.req_id, req_id);

    let claims_manage =
        hdmeal_backend::shared::security::validate_user_token(ValidateUserTokenInput {
            token: &token,
            secret,
            required_scope: scope::MANAGE_USER_INFO,
        })
        .unwrap();
    assert_eq!(claims_manage.scope.len(), 2);
}

#[test]
fn jwt_expired_is_rejected() {
    // Issue a token with TTL well in the past. Current leeway is 30s,
    // so we need exp to be more than 30s in the past to force rejection.
    let secret = "test";
    let req_id = hdmeal_backend::shared::context::new_request_id();
    let token = hdmeal_backend::shared::security::issue_user_token(IssueUserTokenInput {
        secret,
        uid: "KT:u",
        scope: vec![scope::GET_USER_INFO.into()],
        req_id: &req_id,
        ttl: chrono::Duration::seconds(-120),
    })
    .unwrap();
    let err = hdmeal_backend::shared::security::validate_user_token(ValidateUserTokenInput {
        token: &token,
        secret,
        required_scope: scope::GET_USER_INFO,
    })
    .unwrap_err();
    assert_eq!(err.status(), axum::http::StatusCode::UNAUTHORIZED);
}

#[test]
fn dday_format_matches_python() {
    let d = chrono::NaiveDate::from_ymd_opt(2024, 3, 5).unwrap();
    assert_eq!(format_date_label(d), "2024-03-05(화)");
}

#[test]
fn hour_label() {
    assert_eq!(format_hour(0), "오전 12시");
    assert_eq!(format_hour(9), "오전 9시");
    assert_eq!(format_hour(12), "오후 12시");
    assert_eq!(format_hour(21), "오후 9시");
}

#[test]
fn today_kst_window() {
    let today = today_kst_date();
    let start = today - ChronoDuration::days(1);
    let end = today + ChronoDuration::days(7);
    let (start_utc, end_utc) = (kst_midnight_to_utc(start), kst_midnight_to_utc(end));
    assert!(start_utc < end_utc);
}

#[test]
fn intent_kind_dispatch() {
    assert_eq!(
        IntentKind::from_name("블라블라 Briefing 블라블라"),
        IntentKind::Briefing
    );
    assert_eq!(IntentKind::from_name("MealQuery"), IntentKind::Meal);
    assert_eq!(
        IntentKind::from_name("TimetableQuery"),
        IntentKind::Timetable
    );
    assert_eq!(
        IntentKind::from_name("SchoolSchedule"),
        IntentKind::Schedule
    );
    assert_eq!(
        IntentKind::from_name("WaterTemperatureInfo"),
        IntentKind::WaterTemperature
    );
    assert_eq!(
        IntentKind::from_name("UserSettingsCard"),
        IntentKind::UserSettings
    );
    assert_eq!(
        IntentKind::from_name("ModifyUserInfo"),
        IntentKind::ModifyUserInfo
    );
    assert_eq!(IntentKind::from_name("UnknownBlock"), IntentKind::Unknown);
}

#[test]
fn kakao_action_date_period() {
    // params.date_period 가 `{"from":{"date":"20240301"}, "to":{"date":"20240307"}}` 형태
    let mut params = HashMap::new();
    params.insert(
        "date_period".to_string(),
        serde_json::json!({
            "from": {"date": "2024-03-01"},
            "to": {"date": "2024-03-07"},
        }),
    );
    let action = KakaoAction { params };
    let p = action.get_date_period().unwrap();
    assert_eq!(p.from.as_deref(), Some("2024-03-01"));
    assert_eq!(p.to.as_deref(), Some("2024-03-07"));
}

#[test]
fn kakao_action_date_stringified_json() {
    // Kakao 가 보내는 형태: `params.date` 가 `{"date":"20240301"}` 의 JSON 문자열
    let mut params = HashMap::new();
    params.insert(
        "date".to_string(),
        serde_json::Value::String(r#"{"date":"2024-03-01"}"#.to_string()),
    );
    let action = KakaoAction { params };
    assert_eq!(action.get_date().as_deref(), Some("2024-03-01"));
}

#[test]
fn kakao_action_grade_class() {
    let mut params = HashMap::new();
    params.insert(
        "grade".to_string(),
        serde_json::Value::String("2학년".to_string()),
    );
    params.insert("class".to_string(), serde_json::json!(7));
    let action = KakaoAction { params };
    assert_eq!(action.get_grade(), Some(2));
    assert_eq!(action.get_class(), Some(7));
}

#[test]
fn kakao_request_deserialize() {
    let raw = serde_json::json!({
        "userRequest": {"user": {"id": "ext-1"}, "utterance": "급식"},
        "intent": {"name": "MealQuery"},
        "action": {"params": {"date": "2024-03-01"}}
    });
    let req: KakaoSkillRequest = serde_json::from_value(raw).unwrap();
    assert_eq!(req.user_request.user.id, "ext-1");
    assert_eq!(req.intent.name, "MealQuery");
}

#[test]
fn update_user_settings_deserialize_int() {
    let raw = serde_json::json!({
        "user_grade": 2,
        "user_class": 5,
        "preferences": {"AllergyInfo": "Number"}
    });
    let req: UpdateUserSettingsRequest = serde_json::from_value(raw).unwrap();
    assert_eq!(req.user_grade, 2);
    assert_eq!(req.user_class, 5);
    assert_eq!(req.preferences.get("AllergyInfo").unwrap(), "Number");
}

#[test]
fn update_user_settings_deserialize_korean_string() {
    let raw = serde_json::json!({
        "user_grade": "2학년",
        "user_class": "5반",
        "preferences": {"AllergyInfo": "FullText"}
    });
    let req: UpdateUserSettingsRequest = serde_json::from_value(raw).unwrap();
    assert_eq!(req.user_grade, 2);
    assert_eq!(req.user_class, 5);
}

#[test]
fn skill_token_constant_time_all_checked() {
    use hdmeal_backend::shared::security::authorize_skill_token;
    let allowed = vec!["alpha".to_string(), "beta".to_string()];
    assert!(authorize_skill_token(Some("alpha"), &allowed));
    assert!(authorize_skill_token(Some("beta"), &allowed));
    assert!(!authorize_skill_token(Some("gamma"), &allowed));
    assert!(!authorize_skill_token(None, &allowed));
    // 빈 allowed 벡터에 대해 항상 false
    assert!(!authorize_skill_token(Some("any"), &[]));
}

#[test]
fn metrics_atomic_counter() {
    use hdmeal_backend::shared::metrics::Metrics;
    let m = Metrics::new();
    m.record_request("/healthz", "GET", 200);
    m.record_request("/healthz", "GET", 200);
    m.record_request("/healthz", "GET", 500);
    let rendered = m.render();
    assert!(rendered.contains(r#"path="/healthz",method="GET",status="200"} 2"#));
    assert!(rendered.contains(r#"path="/healthz",method="GET",status="500"} 1"#));
}
