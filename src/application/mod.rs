//! Use-case / application service 계층.
//!
//! 트랜스포트 계층이 의존하는 단일 진입점.

pub mod chatbot;
pub mod ingestion_service;
pub mod user_service;

use std::sync::Arc;

use crate::infrastructure::neis::auxiliary::{KmaClient, SeoulWaterClient};
use crate::infrastructure::neis::neis::NeisClient;
use crate::repository::DataService;

/// 모든 application service 가 공유하는 의존성 묶음.
#[derive(Clone)]
pub struct AppContext {
    pub data: Arc<DataService>,
    pub neis: Arc<NeisClient>,
    pub kma: Arc<KmaClient>,
    pub seoul_water: Arc<SeoulWaterClient>,
    pub user_service: Arc<user_service::UserService>,
    pub ingestion: Arc<ingestion_service::IngestionService>,
    pub chatbot: Arc<chatbot::Service>,
    pub config: Arc<crate::config::AppConfig>,
}

impl AppContext {
    pub fn new(
        data: Arc<DataService>,
        neis: Arc<NeisClient>,
        kma: Arc<KmaClient>,
        seoul_water: Arc<SeoulWaterClient>,
        config: Arc<crate::config::AppConfig>,
    ) -> Self {
        let user_service = Arc::new(user_service::UserService::new(data.clone()));
        let ingestion = Arc::new(ingestion_service::IngestionService::new(
            data.clone(),
            neis.clone(),
        ));
        let chatbot = Arc::new(chatbot::Service::new(
            data.clone(),
            user_service.clone(),
            ingestion.clone(),
            kma.clone(),
            seoul_water.clone(),
            (*config).clone(),
        ));
        Self {
            data,
            neis,
            kma,
            seoul_water,
            user_service,
            ingestion,
            chatbot,
            config,
        }
    }
}
