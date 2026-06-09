//! 챗봇 메시지 / 카카오 요청·응답 타입.

use std::collections::HashMap;
use std::fmt::Write as _;

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

pub fn format_menu_with_allergies(name: &str, allergies: &[i32], mode: &str) -> String {
    let clean_name = name.trim_start_matches('⭐').trim();
    match mode {
        "None" => clean_name.to_owned(),
        "FullText" => {
            let labels: Vec<&str> = allergies
                .iter()
                .filter_map(|&a| ALLERGY_LABELS.get(a as usize).copied())
                .collect();
            if labels.is_empty() {
                clean_name.to_owned()
            } else {
                format!("{clean_name}({})", labels.join(", "))
            }
        }
        _ => format_menu_with_numbers(clean_name, allergies),
    }
}

fn format_menu_with_numbers(name: &str, allergies: &[i32]) -> String {
    if allergies.is_empty() {
        return name.to_owned();
    }

    let mut out = String::with_capacity(name.len() + allergies.len() * 4 + 2);
    out.push_str(name);
    out.push('(');
    for (idx, allergy) in allergies.iter().enumerate() {
        if idx > 0 {
            out.push_str(", ");
        }
        let _ = write!(out, "{allergy}");
    }
    out.push(')');
    out
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intent_kind_all_variants() {
        assert_eq!(
            IntentKind::from_name("ModifyUserInfoBlock"),
            IntentKind::ModifyUserInfo
        );
        assert_eq!(IntentKind::from_name("Briefing"), IntentKind::Briefing);
        assert_eq!(IntentKind::from_name("MealBlock"), IntentKind::Meal);
        assert_eq!(
            IntentKind::from_name("TimetableHandler"),
            IntentKind::Timetable
        );
        assert_eq!(
            IntentKind::from_name("ScheduleIntent"),
            IntentKind::Schedule
        );
        assert_eq!(
            IntentKind::from_name("WaterTemperature"),
            IntentKind::WaterTemperature
        );
        assert_eq!(
            IntentKind::from_name("UserSettings"),
            IntentKind::UserSettings
        );
        assert_eq!(IntentKind::from_name("SomethingElse"), IntentKind::Unknown);
        assert_eq!(IntentKind::from_name(""), IntentKind::Unknown);
    }

    #[test]
    fn intent_kind_priority_modify_over_others() {
        assert_eq!(
            IntentKind::from_name("ModifyUserInfoBriefing"),
            IntentKind::ModifyUserInfo
        );
    }

    #[test]
    fn parse_date_valid() {
        assert!(parse_date("2026-06-09").is_some());
    }

    #[test]
    fn parse_date_invalid() {
        assert!(parse_date("not-a-date").is_none());
        assert!(parse_date("2026/06/09").is_none());
        assert!(parse_date("06-09-2026").is_none());
        assert!(parse_date("").is_none());
    }

    #[test]
    fn format_menu_with_allergies_none_mode() {
        assert_eq!(format_menu_with_allergies("⭐밥", &[], "None"), "밥");
        assert_eq!(format_menu_with_allergies("김치", &[], "None"), "김치");
    }

    #[test]
    fn format_menu_with_allergies_fulltext_with_labels() {
        let result = format_menu_with_allergies("밥", &[1, 2, 4], "FullText");
        assert!(result.contains("난류"));
        assert!(result.contains("우유"));
        assert!(result.contains("땅콩"));
    }

    #[test]
    fn format_menu_with_allergies_fulltext_no_allergies() {
        assert_eq!(format_menu_with_allergies("밥", &[], "FullText"), "밥");
    }

    #[test]
    fn format_menu_with_allergies_fulltext_unknown_allergy_code() {
        let result = format_menu_with_allergies("밥", &[99], "FullText");
        assert_eq!(result, "밥");
    }

    #[test]
    fn format_menu_with_allergies_number_mode() {
        let result = format_menu_with_allergies("밥", &[1, 2], "Number");
        assert_eq!(result, "밥(1, 2)");
    }

    #[test]
    fn format_menu_with_allergies_number_no_allergies() {
        assert_eq!(format_menu_with_allergies("밥", &[], "Number"), "밥");
    }

    #[test]
    fn format_menu_strips_star_prefix() {
        assert_eq!(
            format_menu_with_allergies("⭐특급밥", &[], "None"),
            "특급밥"
        );
    }

    #[test]
    fn extract_date_field_plain_string() {
        let v = serde_json::Value::String("2024-03-01".to_string());
        assert_eq!(extract_date_field(&v), Some("2024-03-01".to_string()));
    }

    #[test]
    fn extract_date_field_stringified_json() {
        let v = serde_json::Value::String(r#"{"date":"2024-03-01"}"#.to_string());
        assert_eq!(extract_date_field(&v), Some("2024-03-01".to_string()));
    }

    #[test]
    fn extract_date_field_object() {
        let v = serde_json::json!({"date": "2024-03-01"});
        assert_eq!(extract_date_field(&v), Some("2024-03-01".to_string()));
    }

    #[test]
    fn extract_date_field_number_returns_none() {
        let v = serde_json::Value::Number(42.into());
        assert_eq!(extract_date_field(&v), None);
    }

    #[test]
    fn extract_digits_from_korean() {
        assert_eq!(extract_digits("2학년"), Some(2));
        assert_eq!(extract_digits("3반"), Some(3));
    }

    #[test]
    fn extract_digits_from_number() {
        assert_eq!(extract_digits("5"), Some(5));
    }

    #[test]
    fn extract_digits_no_digits() {
        assert_eq!(extract_digits("abc"), None);
    }

    #[test]
    fn kakao_skill_response_text_serialization() {
        let messages = vec![Message::Text("hello".to_string())];
        let resp = KakaoSkillResponse::from_messages(messages);
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["version"], "2.0");
        assert_eq!(
            json["template"]["outputs"][0]["simpleText"]["text"],
            "hello"
        );
    }

    #[test]
    fn kakao_skill_response_card_with_web_button() {
        let messages = vec![Message::Card(CardMessage {
            title: "t".to_string(),
            description: "d".to_string(),
            thumbnail_url: Some("http://img".to_string()),
            buttons: vec![CardButton::Web {
                title: "open".to_string(),
                url: "http://link".to_string(),
            }],
        })];
        let resp = KakaoSkillResponse::from_messages(messages);
        let json = serde_json::to_value(&resp).unwrap();
        let card = &json["template"]["outputs"][0]["basicCard"];
        assert_eq!(card["title"], "t");
        assert_eq!(card["thumbnail"]["imageUrl"], "http://img");
        assert_eq!(card["buttons"][0]["action"], "webLink");
    }

    #[test]
    fn kakao_skill_response_card_with_message_button() {
        let messages = vec![Message::Card(CardMessage {
            title: "t".to_string(),
            description: "d".to_string(),
            thumbnail_url: None,
            buttons: vec![CardButton::Message {
                title: "click".to_string(),
                postback: Some("payload".to_string()),
            }],
        })];
        let resp = KakaoSkillResponse::from_messages(messages);
        let json = serde_json::to_value(&resp).unwrap();
        let btn = &json["template"]["outputs"][0]["basicCard"]["buttons"][0];
        assert_eq!(btn["action"], "message");
        assert_eq!(btn["messageText"], "payload");
    }

    #[test]
    fn kakao_skill_response_card_message_button_no_postback_uses_title() {
        let messages = vec![Message::Card(CardMessage {
            title: "t".to_string(),
            description: "d".to_string(),
            thumbnail_url: None,
            buttons: vec![CardButton::Message {
                title: "click".to_string(),
                postback: None,
            }],
        })];
        let resp = KakaoSkillResponse::from_messages(messages);
        let json = serde_json::to_value(&resp).unwrap();
        let btn = &json["template"]["outputs"][0]["basicCard"]["buttons"][0];
        assert_eq!(btn["messageText"], "click");
    }

    #[test]
    fn kakao_action_get_date_missing() {
        let action = KakaoAction::default();
        assert!(action.get_date().is_none());
    }

    #[test]
    fn kakao_action_get_date_period_missing() {
        let action = KakaoAction::default();
        assert!(action.get_date_period().is_none());
    }

    #[test]
    fn kakao_action_get_grade_number() {
        let mut params = HashMap::new();
        params.insert("grade".to_string(), serde_json::json!(3));
        let action = KakaoAction { params };
        assert_eq!(action.get_grade(), Some(3));
    }

    #[test]
    fn kakao_action_get_grade_invalid_type() {
        let mut params = HashMap::new();
        params.insert("grade".to_string(), serde_json::json!(true));
        let action = KakaoAction { params };
        assert_eq!(action.get_grade(), None);
    }

    #[test]
    fn kakao_action_get_class_string_with_digits() {
        let mut params = HashMap::new();
        params.insert(
            "class".to_string(),
            serde_json::Value::String("5반".to_string()),
        );
        let action = KakaoAction { params };
        assert_eq!(action.get_class(), Some(5));
    }

    #[test]
    fn date_period_default() {
        let dp = DatePeriod::default();
        assert!(dp.from.is_none());
        assert!(dp.to.is_none());
    }
}
