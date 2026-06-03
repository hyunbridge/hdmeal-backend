//! `/api/app/*` 와 챗봇 응답 DTO.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate};
use serde::{Deserialize, Serialize};

use crate::domain::{MealDocument, ScheduleDocument, TimetableDocument};

// ---------- /api/app/days ----------

#[derive(Debug, Deserialize)]
pub struct DaysQuery {
    pub from: Option<String>,
    pub to: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DaysResponse {
    #[serde(rename = "requestId")]
    pub request_id: String,
    pub range: DateRange,
    pub data: Vec<DayEntry>,
}

#[derive(Debug, Serialize)]
pub struct DateRange {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Serialize)]
pub struct DayEntry {
    pub date: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meal: Option<MealView>,
    #[serde(default)]
    pub schedule: Vec<ScheduleEntryView>,
    pub timetable: TimetableView,
}

#[derive(Debug, Serialize)]
pub struct MealView {
    pub items: Vec<MealItemView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kcal: Option<f64>,
    #[serde(rename = "updatedAt", skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct MealItemView {
    pub name: String,
    pub allergies: Vec<i32>,
}

#[derive(Debug, Serialize)]
pub struct ScheduleEntryView {
    pub name: String,
    pub grades: Vec<i32>,
}

#[derive(Debug, Serialize)]
pub struct TimetableView {
    pub lessons: BTreeMap<String, BTreeMap<String, Vec<String>>>,
    #[serde(rename = "updatedAt", skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

// ---------- /api/app/days/{day} ----------

#[derive(Debug, Serialize)]
pub struct DayResponse {
    #[serde(rename = "requestId")]
    pub request_id: String,
    pub data: DayEntry,
}

// ---------- /api/app/meta ----------

#[derive(Debug, Serialize)]
pub struct MetaResponse {
    #[serde(rename = "requestId")]
    pub request_id: String,
    pub data: MetaData,
}

#[derive(Debug, Serialize)]
pub struct MetaData {
    pub version: String,
    pub build: u32,
    pub debug: bool,
}

// ---------- /cache/healthcheck ----------

#[derive(Debug, Serialize)]
pub struct CacheHealthcheckResponse {
    pub timetable: CacheHealthStatus,
    pub weather: CacheHealthStatus,
    pub water_temperature: CacheHealthStatus,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum CacheHealthStatus {
    Valid,
    Expired,
    NotFound,
}

// ---------- /skill/ ----------

pub use crate::application::chatbot::types::KakaoSkillResponse as SkillResponse;

// ---------- Converters ----------

impl MealDocument {
    pub fn into_view(self) -> MealView {
        let updated_at = crate::shared::timezone::to_kst_iso(&self.created_at);
        MealView {
            items: self
                .menus
                .into_iter()
                .map(|m| MealItemView {
                    name: m.name,
                    allergies: m.allergies,
                })
                .collect(),
            kcal: self.calories,
            updated_at: Some(updated_at),
        }
    }
}

impl ScheduleDocument {
    pub fn into_view(self) -> Vec<ScheduleEntryView> {
        self.entries
            .into_iter()
            .map(|e| ScheduleEntryView {
                name: e.name,
                grades: e.grades,
            })
            .collect()
    }
}

impl TimetableDocument {
    pub fn into_view(self) -> TimetableView {
        TimetableView {
            lessons: self.lessons,
            updated_at: Some(crate::shared::timezone::to_kst_iso(&self.created_at)),
        }
    }
}

// ---------- Date parsing ----------

pub fn parse_date_param(s: &str, field: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|_| format!("'{field}' 형식이 올바르지 않습니다. (YYYY-MM-DD)"))
}

pub fn parse_optional_date_param(s: Option<&str>) -> Result<Option<NaiveDate>, String> {
    match s {
        None | Some("") => Ok(None),
        Some(value) => NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map(Some)
            .map_err(|_| "잘못된 날짜 형식입니다. (YYYY-MM-DD)".to_string()),
    }
}

pub fn parse_updated_at(dt: &DateTime<chrono::Utc>) -> String {
    crate::shared::timezone::to_kst_iso(dt)
}
