//! Intent dispatch.

use crate::application::chatbot::types::{IntentKind, KakaoSkillRequest, Message};
use crate::error::HDMealResult;

use super::Service;

pub async fn dispatch(svc: &Service, req: &KakaoSkillRequest) -> HDMealResult<Vec<Message>> {
    let kind = IntentKind::from_name(&req.intent.name);
    let platform = "KT";
    let external_id = &req.user_request.user.id;

    match kind {
        IntentKind::Briefing => {
            super::briefing_timetable::handle_briefing(svc, req, platform, external_id).await
        }
        IntentKind::Meal => {
            super::meal_schedule::handle_meal(svc, req, platform, external_id).await
        }
        IntentKind::Timetable => {
            super::briefing_timetable::handle_timetable(svc, req, platform, external_id).await
        }
        IntentKind::Schedule => {
            super::meal_schedule::handle_schedule(svc, req, platform, external_id).await
        }
        IntentKind::WaterTemperature => super::weather_user::handle_water_temperature(svc).await,
        IntentKind::UserSettings => {
            super::weather_user::handle_user_settings(svc, platform, external_id).await
        }
        IntentKind::ModifyUserInfo => {
            super::weather_user::handle_modify_user_info(svc, req, platform, external_id).await
        }
        IntentKind::Unknown => Ok(vec![Message::Text(format!(
            "잘못된 요청입니다.\n요청 ID: {}",
            crate::shared::context::current_request_id()
                .unwrap_or_else(crate::shared::context::new_request_id)
        ))]),
    }
}
