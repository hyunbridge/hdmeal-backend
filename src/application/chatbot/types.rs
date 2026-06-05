//! 챗봇 메시지 / 카카오 요청·응답 타입.

use std::collections::HashMap;

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

// ----------------- Shared constants -----------------

pub const ALLERGY_LABELS: &[&str] = &[
    "",
    "난류",
    "우유",
    "메밀",
    "땅콩",
    "대두",
    "밀",
    "고등어",
    "게",
    "새우",
    "돼지고기",
    "복숭아",
    "토마토",
    "아황산류",
    "호두",
    "닭고기",
    "쇠고기",
    "오징어",
    "조개류",
];

pub fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

// ----------------- Kakao request -----------------

#[derive(Debug, Clone, Deserialize)]
pub struct KakaoSkillRequest {
    #[serde(rename = "userRequest")]
    pub user_request: KakaoUserRequest,
    pub intent: KakaoIntent,
    pub action: KakaoAction,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KakaoUserRequest {
    pub user: KakaoUser,
    pub utterance: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KakaoUser {
    pub id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KakaoIntent {
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct KakaoAction {
    #[serde(default)]
    pub params: HashMap<String, serde_json::Value>,
}

impl KakaoAction {
    /// `params.date` 가 Kakao 에서 `{"date": "YYYY-MM-DD"}` 형태의 JSON 문자열로
    /// 들어오는 경우를 정규화.
    pub fn get_date(&self) -> Option<String> {
        let v = self.params.get("date")?;
        extract_date_field(v)
    }

    /// `params.date_period.from.date` / `.to.date` 정규화.
    pub fn get_date_period(&self) -> Option<DatePeriod> {
        let v = self.params.get("date_period")?;
        let obj = v.as_object()?;
        let from = obj.get("from").and_then(extract_date_field);
        let to = obj.get("to").and_then(extract_date_field);
        Some(DatePeriod { from, to })
    }

    pub fn get_grade(&self) -> Option<i32> {
        let v = self.params.get("grade")?;
        match v {
            serde_json::Value::Number(n) => n.as_i64().map(|x| x as i32),
            serde_json::Value::String(s) => extract_digits(s),
            _ => None,
        }
    }

    pub fn get_class(&self) -> Option<i32> {
        let v = self.params.get("class")?;
        match v {
            serde_json::Value::Number(n) => n.as_i64().map(|x| x as i32),
            serde_json::Value::String(s) => extract_digits(s),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct DatePeriod {
    pub from: Option<String>,
    pub to: Option<String>,
}

fn extract_date_field(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => {
            // Kakao 가 `{"date":"YYYY-MM-DD"}` 를 문자열로 보내는 케이스.
            if let Ok(inner) = serde_json::from_str::<serde_json::Value>(s) {
                if let Some(d) = inner.get("date").and_then(|x| x.as_str()) {
                    return Some(d.to_string());
                }
            }
            Some(s.clone())
        }
        serde_json::Value::Object(_) => v.get("date").and_then(|x| x.as_str()).map(String::from),
        _ => None,
    }
}

fn extract_digits(s: &str) -> Option<i32> {
    let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

// ----------------- Internal messages -----------------

#[derive(Debug, Clone)]
pub enum Message {
    Text(String),
    Card(CardMessage),
}

#[derive(Debug, Clone)]
pub struct CardMessage {
    pub title: String,
    pub description: String,
    pub thumbnail_url: Option<String>,
    pub buttons: Vec<CardButton>,
}

#[derive(Debug, Clone)]
pub enum CardButton {
    Web {
        title: String,
        url: String,
    },
    Message {
        title: String,
        postback: Option<String>,
    },
}

// ----------------- Kakao response -----------------
//
// Kakao i Open Builder skill response 의 outputs 배열은 각 원소가
//   `{ "<type>": { <content> } }` 형태의 key-wrapped 객체.
// serde derive 로 정확히 표현하기 까다로워서 (key-value 쌍이 output type 으로
// 결정되는 패턴), outputs 만 `serde_json::Value` 로 직접 빌드하고
// `KakaoSkillResponse` 의 wrapper 는 그대로 derive Serialize.
// 이렇게 하면:
//   1. KakaoOutput/KakaoButton/... 등 별도 미러 타입이 필요 없음
//   2. `Vec<Value> -> Vec<KakaoOutput>` 의 불필요한 직렬화/역직렬화 제거
//   3. responses 매크로 / json! 가 곧 JSON 표현이라 코드와 결과가 1:1

#[derive(Debug, Serialize)]
pub struct KakaoSkillResponse {
    pub version: &'static str,
    pub template: KakaoTemplate,
}

#[derive(Debug, Serialize)]
pub struct KakaoTemplate {
    pub outputs: Vec<serde_json::Value>,
}

impl KakaoSkillResponse {
    pub fn from_messages(messages: Vec<Message>) -> Self {
        let mut outputs: Vec<serde_json::Value> = Vec::with_capacity(messages.len());
        for m in messages {
            match m {
                Message::Text(text) => {
                    outputs.push(serde_json::json!({
                        "simpleText": { "text": text }
                    }));
                }
                Message::Card(card) => {
                    let mut bc = serde_json::Map::with_capacity(4);
                    bc.insert("title".into(), serde_json::Value::String(card.title));
                    bc.insert(
                        "description".into(),
                        serde_json::Value::String(card.description),
                    );
                    if let Some(url) = card.thumbnail_url {
                        bc.insert("thumbnail".into(), serde_json::json!({ "imageUrl": url }));
                    }
                    if !card.buttons.is_empty() {
                        let buttons: Vec<serde_json::Value> = card
                            .buttons
                            .into_iter()
                            .map(|b| match b {
                                CardButton::Web { title, url } => serde_json::json!({
                                    "action": "webLink",
                                    "label": title,
                                    "webLinkUrl": url,
                                }),
                                CardButton::Message { title, postback } => {
                                    let text = postback.unwrap_or_else(|| title.clone());
                                    serde_json::json!({
                                        "action": "message",
                                        "label": title,
                                        "messageText": text,
                                    })
                                }
                            })
                            .collect();
                        bc.insert("buttons".into(), serde_json::Value::Array(buttons));
                    }
                    outputs.push(serde_json::json!({ "basicCard": bc }));
                }
            }
        }
        Self {
            version: "2.0",
            template: KakaoTemplate { outputs },
        }
    }
}

// ----------------- Intent -----------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntentKind {
    Briefing,
    Meal,
    Timetable,
    Schedule,
    WaterTemperature,
    UserSettings,
    ModifyUserInfo,
    Unknown,
}

impl IntentKind {
    /// intent name 으로부터 어떤 kind 인지 판정. substring + 우선순위.
    pub fn from_name(name: &str) -> Self {
        if name.contains("ModifyUserInfo") {
            Self::ModifyUserInfo
        } else if name.contains("Briefing") {
            Self::Briefing
        } else if name.contains("Meal") {
            Self::Meal
        } else if name.contains("Timetable") {
            Self::Timetable
        } else if name.contains("Schedule") {
            Self::Schedule
        } else if name.contains("WaterTemperature") {
            Self::WaterTemperature
        } else if name.contains("UserSettings") {
            Self::UserSettings
        } else {
            Self::Unknown
        }
    }
}
