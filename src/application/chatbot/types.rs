//! 챗봇 메시지 / 카카오 요청·응답 타입.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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
        let obj = v.as_object()?.clone();
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

#[derive(Debug, Serialize)]
pub struct KakaoSkillResponse {
    pub version: &'static str,
    pub template: KakaoTemplate,
}

#[derive(Debug, Serialize)]
pub struct KakaoTemplate {
    pub outputs: Vec<KakaoOutput>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "simpleText", rename_all = "camelCase")]
pub struct KakaoSimpleText {
    pub text: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KakaoThumbnail {
    pub image_url: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "camelCase")]
pub enum KakaoButton {
    #[serde(rename = "webLink")]
    WebLink { label: String, web_link_url: String },
    #[serde(rename = "message")]
    Message { label: String, message_text: String },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KakaoBasicCard {
    pub title: String,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumbnail: Option<KakaoThumbnail>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buttons: Option<Vec<KakaoButton>>,
}

impl KakaoSkillResponse {
    pub fn from_messages(messages: Vec<Message>) -> Self {
        let mut outputs: Vec<serde_json::Value> = Vec::new();
        for m in messages {
            match m {
                Message::Text(text) => {
                    outputs.push(serde_json::json!({
                        "simpleText": {"text": text}
                    }));
                }
                Message::Card(card) => {
                    let mut bc = serde_json::json!({
                        "title": card.title,
                        "description": card.description,
                    });
                    if let Some(url) = card.thumbnail_url {
                        bc["thumbnail"] = serde_json::json!({"imageUrl": url});
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
                                    let text = postback.unwrap_or(title.clone());
                                    serde_json::json!({
                                        "action": "message",
                                        "label": title,
                                        "messageText": text,
                                    })
                                }
                            })
                            .collect();
                        bc["buttons"] = serde_json::Value::Array(buttons);
                    }
                    outputs.push(serde_json::json!({
                        "basicCard": bc
                    }));
                }
            }
        }
        // serde_json::Value 로 모았다가 최종적으로 KakaoOutput 구조로 변환.
        // 단순화를 위해 `outputs` 를 `Vec<KakaoOutput>` 으로 한 번 더 deserialize.
        let outputs: Vec<KakaoOutput> =
            serde_json::from_value(serde_json::Value::Array(outputs)).unwrap_or_default();
        Self {
            version: "2.0",
            template: KakaoTemplate { outputs },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum KakaoOutput {
    #[serde(rename = "simpleText")]
    SimpleText { text: String },
    #[serde(rename = "basicCard")]
    BasicCard {
        title: String,
        description: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        thumbnail: Option<KakaoThumbnail>,
        #[serde(skip_serializing_if = "Option::is_none")]
        buttons: Option<Vec<KakaoButton>>,
    },
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
        // 우선순위: 더 긴 substring 부터 (예: "ModifyUserInfo" 가 "UserInfo" 보다 먼저 매칭되어야 함)
        // 단, Go 의 `normalizeIntent` 는 단순 contains 라는 점에 주의. 핵심 7개가 모두
        // disjoint 한 한 단어를 포함하므로 단순 contains 로 충분하다.
        if name.contains("Briefing") {
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
        } else if name.contains("ModifyUserInfo") {
            Self::ModifyUserInfo
        } else {
            Self::Unknown
        }
    }
}
