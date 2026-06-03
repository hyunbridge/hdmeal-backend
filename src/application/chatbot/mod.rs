//! 카카오 챗봇 skill 서버.
//!
//! - [`Service`] 가 [`crate::transport::http::chatbot::handle_skill`] 로부터 호출되는
//!   단일 진입점.
//! - [`core::dispatch`] 가 Kakao 요청을 [`Message`] 시퀀스로 변환.
//! - intent 별 로직은 `briefing_timetable`, `meal_schedule`, `weather_user`,
//!   `sync_helpers` 모듈에 분리.

pub mod briefing_timetable;
pub mod core;
pub mod meal_schedule;
pub mod sync_helpers;
pub mod types;
pub mod weather_user;

use std::sync::Arc;

use crate::application::ingestion_service::IngestionService;
use crate::application::user_service::UserService;
use crate::config::AppConfig;
use crate::infrastructure::neis::auxiliary::{KmaClient, SeoulWaterClient};
use crate::repository::DataService;

#[derive(Clone)]
pub struct Service {
    pub data: Arc<DataService>,
    pub users: Arc<UserService>,
    pub ingestion: Arc<IngestionService>,
    pub kma: Arc<KmaClient>,
    pub seoul_water: Arc<SeoulWaterClient>,
    pub config: AppConfig,
}

impl Service {
    pub fn new(
        data: Arc<DataService>,
        users: Arc<UserService>,
        ingestion: Arc<IngestionService>,
        kma: Arc<KmaClient>,
        seoul_water: Arc<SeoulWaterClient>,
        config: AppConfig,
    ) -> Self {
        Self {
            data,
            users,
            ingestion,
            kma,
            seoul_water,
            config,
        }
    }

    /// Kakao 요청을 받아 intent 별 handler 로 위임.
    pub async fn dispatch_internal(
        &self,
        req: &crate::application::chatbot::types::KakaoSkillRequest,
    ) -> crate::error::HDMealResult<Vec<crate::application::chatbot::types::Message>> {
        self::core::dispatch(self, req).await
    }
}
