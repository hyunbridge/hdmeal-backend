//! Weather, WaterTemperature, UserSettings, ModifyUserInfo intent 핸들러.

use chrono::{Duration, Utc};

use crate::application::chatbot::sync_helpers::{ensure_water_temperature, hour_label_from_ts};
use crate::application::chatbot::types::{CardButton, CardMessage, KakaoSkillRequest, Message};
use crate::application::chatbot::Service;
use crate::domain::WeatherDocument;
use crate::error::HDMealResult;
use crate::shared::context::new_request_id;
use crate::shared::security::{issue_user_token, scope, IssueUserTokenInput};
use crate::shared::timezone::{format_date_label, today_kst_date, KST};

/// KMA 응답을 briefing 텍스트로.
pub fn weather_briefing_text(date_label: &str, w: &WeatherDocument) -> String {
    format!(
        "🌡️ {} 최소/최대 기온: {}℃/{}℃\n\n등굣길 예상 날씨: {}\n🌡️ 기온: {}℃\n🌦️ 강수 형태: {}\n❔ 강수 확률: {}%\n💧 습도: {}%",
        date_label,
        w.temp_min,
        w.temp_max,
        w.sky,
        w.temp,
        w.pty,
        w.precip_probability,
        w.humidity
    )
}

pub async fn handle_water_temperature(svc: &Service) -> HDMealResult<Vec<Message>> {
    let ttl = svc.config.cache_health_water_temp_ttl;
    let Some(doc) = ensure_water_temperature(svc, ttl).await else {
        return Ok(vec![Message::Text(
            "측정소 또는 서버 오류입니다.".to_string(),
        )]);
    };
    let kst_date = doc.timestamp.with_timezone(&KST).date_naive();
    let hour_label = hour_label_from_ts(doc.timestamp);
    let text = format!(
        "{} {} 측정자료:\n한강 수온은 {}°C 입니다.",
        format_date_label(kst_date),
        hour_label,
        doc.temperature_c
    );
    Ok(vec![Message::Text(text)])
}

pub async fn handle_user_settings(
    svc: &Service,
    platform: &str,
    external_id: &str,
) -> HDMealResult<Vec<Message>> {
    let req_id = new_request_id();
    let uid = format!("{platform}:{external_id}");
    let token = issue_user_token(IssueUserTokenInput {
        secret: &svc.config.jwt_secret,
        uid: &uid,
        scope: vec![
            scope::GET_USER_INFO.into(),
            scope::MANAGE_USER_INFO.into(),
            scope::GET_USAGE_DATA.into(),
            scope::DELETE_USAGE_DATA.into(),
        ],
        req_id: &req_id,
        ttl: chrono::Duration::minutes(10),
    })?;

    // base URL 에 token 쿼리 추가
    let mut url = svc.config.base_url.clone();
    {
        let mut qp = url.query_pairs_mut();
        qp.append_pair("token", &token);
    }

    let card = CardMessage {
        title: "내 정보 관리".to_string(),
        description: "아래 버튼을 클릭해 관리 페이지로 접속해 주세요.\n링크는 10분 뒤 만료됩니다."
            .to_string(),
        thumbnail_url: None,
        buttons: vec![CardButton::Web {
            title: "내 정보 관리".to_string(),
            url: url.to_string(),
        }],
    };
    Ok(vec![Message::Card(card)])
}

pub async fn handle_modify_user_info(
    svc: &Service,
    req: &KakaoSkillRequest,
    platform: &str,
    external_id: &str,
) -> HDMealResult<Vec<Message>> {
    let grade = req.action.get_grade();
    let class_no = req.action.get_class();

    let (g, c) = match (grade, class_no) {
        (Some(g), Some(c)) => (g, c),
        _ => {
            return Ok(vec![Message::Text(
                "올바른 숫자를 입력해 주세요.".to_string(),
            )]);
        }
    };
    if !(1..=svc.config.num_of_grades as i32).contains(&g) {
        return Ok(vec![Message::Text(
            "올바른 학년/반을 입력해 주세요.".to_string(),
        )]);
    }
    if !(1..=svc.config.num_of_classes as i32).contains(&c) {
        return Ok(vec![Message::Text(
            "올바른 학년/반을 입력해 주세요.".to_string(),
        )]);
    }

    let input = crate::application::user_service::UpdateUserInput {
        grade: Some(Some(g)),
        class_no: Some(Some(c)),
        preferences: std::collections::HashMap::new(),
    };
    svc.users
        .update_user(
            platform,
            external_id,
            input,
            svc.config.num_of_grades,
            svc.config.num_of_classes,
        )
        .await?;
    Ok(vec![Message::Text("저장되었습니다.".to_string())])
}

// Quiet unused-import warnings on Datelike/Duration/Utc in some build configs.
#[allow(dead_code)]
fn _use() {
    let _ = (today_kst_date(), Utc::now(), Duration::hours(0));
}
