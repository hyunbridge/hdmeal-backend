//! MongoDB 클라이언트 / 컬렉션 핸들.

use std::sync::Arc;

use mongodb::options::ClientOptions;
use mongodb::{Client, Database};

use crate::config::AppConfig;
use crate::domain::{
    MealDocument, ScheduleDocument, TimetableDocument, UserDocument, WaterTemperatureDocument,
    WeatherDocument,
};
use crate::error::HDMealResult;

pub mod data_service;

pub use data_service::{DataService, WeatherUpsert};

const COLL_MEALS: &str = "meals";
const COLL_SCHEDULES: &str = "schedules";
const COLL_TIMETABLES: &str = "timetables";
const COLL_WEATHER: &str = "weather";
const COLL_WATER: &str = "water_temperatures";
const COLL_USERS: &str = "users";

/// MongoDB 클라이언트를 초기화하고 ping 검증.
pub async fn init_client(config: &AppConfig) -> HDMealResult<Arc<Client>> {
    let mut opts = ClientOptions::parse(&config.mongodb_uri).await?;
    opts.app_name = Some(config.app_name.clone());
    opts.connect_timeout = Some(std::time::Duration::from_secs(10));
    opts.server_selection_timeout = Some(std::time::Duration::from_secs(10));

    let client = Arc::new(Client::with_options(opts)?);
    client
        .database(&config.mongodb_database)
        .run_command(mongodb::bson::doc! {"ping": 1})
        .await?;
    Ok(client)
}

/// 컬렉션 핸들 묶음.
#[derive(Clone)]
pub struct Collections {
    pub meals: mongodb::Collection<MealDocument>,
    pub schedules: mongodb::Collection<ScheduleDocument>,
    pub timetables: mongodb::Collection<TimetableDocument>,
    pub weather: mongodb::Collection<WeatherDocument>,
    pub water_temperatures: mongodb::Collection<WaterTemperatureDocument>,
    pub users: mongodb::Collection<UserDocument>,
    pub raw: Database,
}

impl Collections {
    pub fn from_db(db: &Database) -> Self {
        Self {
            meals: db.collection::<MealDocument>(COLL_MEALS),
            schedules: db.collection::<ScheduleDocument>(COLL_SCHEDULES),
            timetables: db.collection::<TimetableDocument>(COLL_TIMETABLES),
            weather: db.collection::<WeatherDocument>(COLL_WEATHER),
            water_temperatures: db.collection::<WaterTemperatureDocument>(COLL_WATER),
            users: db.collection::<UserDocument>(COLL_USERS),
            raw: db.clone(),
        }
    }
}

/// 데이터베이스와 컬렉션 묶음을 반환합니다.
pub fn get_collections(client: &Client, db_name: &str) -> Collections {
    let db = client.database(db_name);
    Collections::from_db(&db)
}
