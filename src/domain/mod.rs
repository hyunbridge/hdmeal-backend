//! MongoDB 도큐먼트 모델.
//!
//! 컬렉션 6 종 (meals, schedules, timetables, weather, water_temperatures, users)
//! 과 그 내부 타입들. BSON 직렬화는
//! [`bson::serde_helpers`] 의 헬퍼를 사용해 `chrono::DateTime<Utc>` 를
//! BSON `DateTime` 으로 저장합니다. HTTP 응답 DTO 는
//! [`crate::transport::http::dto`] 에 별도로 둡니다.

use std::collections::BTreeMap;

use bson::serde_helpers::chrono_datetime_as_bson_datetime;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// `meals` 컬렉션.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MealDocument {
    #[serde(rename = "_id", default, skip_serializing_if = "String::is_empty")]
    pub id: String,

    /// `YYYY-MM-DD`. 문자열로 저장.
    pub date: String,

    pub menus: Vec<MealMenuItem>,

    /// 평문 메뉴명 (legacy 호환용, `data/delicious.txt` ⭐ 마킹 이전 형태).
    #[serde(default)]
    pub menus_plain: Vec<String>,

    pub calories: Option<f64>,

    pub source_hash: Option<String>,

    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MealMenuItem {
    pub name: String,
    #[serde(default)]
    pub allergies: Vec<i32>,
}

/// `schedules` 컬렉션.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleDocument {
    #[serde(rename = "_id", default, skip_serializing_if = "String::is_empty")]
    pub id: String,

    pub date: String,

    pub entries: Vec<ScheduleEntry>,

    pub summary: Option<String>,

    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleEntry {
    pub name: String,
    #[serde(default)]
    pub grades: Vec<i32>,
}

/// `timetables` 컬렉션.
///
/// lessons: outer key = 학년 ("1".."NUM_OF_GRADES"), middle key = 반 ("1".."NUM_OF_CLASSES"),
/// value = 교시별 과목 배열.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimetableDocument {
    #[serde(rename = "_id", default, skip_serializing_if = "String::is_empty")]
    pub id: String,

    pub date: String,

    /// JSON 직렬화 시 결정적 출력을 위해 BTreeMap 사용. 안쪽도 BTreeMap.
    #[serde(default)]
    pub lessons: BTreeMap<String, BTreeMap<String, Vec<String>>>,

    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub created_at: DateTime<Utc>,
}

/// `weather` 컬렉션.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeatherDocument {
    #[serde(rename = "_id", default, skip_serializing_if = "String::is_empty")]
    pub id: String,

    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub timestamp: DateTime<Utc>,

    pub temp: String,
    pub temp_min: String,
    pub temp_max: String,
    pub sky: String,
    pub pty: String,
    pub precip_probability: String,
    pub humidity: String,
    pub first_hour: String,

    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub created_at: DateTime<Utc>,
}

/// `water_temperatures` 컬렉션.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaterTemperatureDocument {
    #[serde(rename = "_id", default, skip_serializing_if = "String::is_empty")]
    pub id: String,

    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub timestamp: DateTime<Utc>,

    pub temperature_c: f64,

    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub created_at: DateTime<Utc>,
}

/// `users` 컬렉션.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserDocument {
    #[serde(rename = "_id", default, skip_serializing_if = "String::is_empty")]
    pub id: String,

    pub platform: String,
    pub external_id: String,

    pub grade: Option<i32>,
    pub class_no: Option<i32>,

    #[serde(default)]
    pub preferences: UserPreferences,

    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub created_at: DateTime<Utc>,

    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UserPreferences {
    #[serde(default, rename = "AllergyInfo")]
    pub allergy_info: String,
}

impl UserPreferences {
    pub fn is_valid_allergy_info(v: &str) -> bool {
        matches!(v, "None" | "Number" | "FullText")
    }
}

/// 사용자가 명시 가능한 preference 키. PATCH 검증에 사용.
pub const ALLOWED_PREFERENCE_KEYS: &[&str] = &["AllergyInfo"];
