//! Briefing / Timetable intent 핸들러.

use chrono::{Datelike, NaiveDate, Timelike, Weekday};

use crate::application::chatbot::sync_helpers::preload_day_bundle;
use crate::application::chatbot::types::{CardButton, CardMessage, KakaoSkillRequest, Message};
use crate::application::chatbot::Service;
use crate::domain::{MealDocument, ScheduleDocument, TimetableDocument, UserPreferences};
use crate::error::HDMealResult;
use crate::shared::timezone::{today_kst_date, KST};

use super::weather_user::weather_briefing_text;

/// Briefing: 오늘 / 내일 브리핑. (저녁 17시 이후면 내일, 그 외는 오늘)
/// 주말이면 "<요일>은 주말 입니다." 반환.
pub async fn handle_briefing(
    svc: &Service,
    _req: &KakaoSkillRequest,
    platform: &str,
    external_id: &str,
) -> HDMealResult<Vec<Message>> {
    let now = chrono::Utc::now().with_timezone(&KST);
    let today = today_kst_date();
    let target_date = if now.hour() >= 17 {
        today + chrono::Duration::days(1)
    } else {
        today
    };
    let date_label = if now.hour() >= 17 { "내일" } else { "오늘" };
    let weekday = target_date.weekday();

    if matches!(weekday, Weekday::Sat | Weekday::Sun) {
        return Ok(vec![Message::Text(format!(
            "{}은 주말 입니다.",
            date_label
        ))]);
    }

    let user = svc.users.ensure_user(platform, external_id).await?;

    // 3초 timeout 으로 데이터 로드
    let (meal, schedule, timetable) = preload_day_bundle(svc, target_date).await;

    let mut msgs: Vec<Message> = Vec::new();

    // 1) 헤더 + 일정
    let header = format!(
        "{}은 {}({}) 입니다.",
        date_label,
        target_date.format("%Y-%m-%d"),
        weekday_ko(target_date)
    );
    let schedule_text = briefing_schedule_text(date_label, schedule.as_ref());
    msgs.push(Message::Text(format!("{header}\n\n{schedule_text}")));

    // 2) 날씨
    if let Some(w) = crate::application::chatbot::sync_helpers::ensure_weather(
        svc,
        svc.config.cache_health_weather_ttl,
    )
    .await
    {
        msgs.push(Message::Text(weather_briefing_text(date_label, &w)));
    } else {
        msgs.push(Message::Text(
            "날씨 서버에 연결하지 못했습니다.\n나중에 다시 시도해 보세요.".to_string(),
        ));
    }

    // 3) 급식 + 시간표
    let meal_text = briefing_meal_text(
        date_label,
        meal.as_ref(),
        schedule.as_ref(),
        &user.preferences,
    );
    let timetable_text = briefing_timetable_text(
        date_label,
        target_date,
        timetable.as_ref(),
        user.grade,
        user.class_no,
    );
    msgs.push(Message::Text(format!("{meal_text}\n\n{timetable_text}")));

    Ok(msgs)
}

/// Timetable: 특정 날짜의 시간표.
/// - grade/class 가 params 에 있으면 사용, 없으면 사용자 정보에서 가져옴.
/// - 둘 다 없으면 "사용자 정보를 찾을 수 없습니다." 카드.
pub async fn handle_timetable(
    svc: &Service,
    req: &KakaoSkillRequest,
    platform: &str,
    external_id: &str,
) -> HDMealResult<Vec<Message>> {
    let explicit_grade = req.action.get_grade();
    let explicit_class = req.action.get_class();
    let date_param = req.action.get_date();
    let period = req.action.get_date_period();

    let mut suggest_to_register = false;
    let (grade, class) = match (explicit_grade, explicit_class) {
        (Some(g), Some(c)) => {
            if platform == "KT" {
                suggest_to_register = true;
            }
            (g, c)
        }
        _ => {
            let user = svc.users.ensure_user(platform, external_id).await?;
            match (user.grade, user.class_no) {
                (Some(g), Some(c)) => (g, c),
                _ => {
                    if platform == "KT" {
                        return Ok(vec![Message::Card(CardMessage {
                            title: "사용자 정보를 찾을 수 없습니다.".to_string(),
                            description: "\"내 정보 관리\"를 눌러 학년/반 정보를 등록 하시거나, \"1학년 1반 시간표 알려줘\"와 같이 조회할 학년/반을 직접 언급해 주세요.".to_string(),
                            thumbnail_url: None,
                            buttons: vec![CardButton::Message {
                                title: "내 정보 관리".to_string(),
                                postback: None,
                            }],
                        })]);
                    }
                    return Ok(vec![Message::Text(
                        "사용자 정보를 찾을 수 없습니다. \"내 정보 관리\"를 눌러 학년/반 정보를 등록해 주세요.".to_string(),
                    )]);
                }
            }
        }
    };

    // 1) date 결정
    let date = if let Some(d) = date_param {
        parse_date(&d).ok_or_else(|| {
            crate::error::HDMealError::bad_request("잘못된 날짜 형식입니다. (YYYY-MM-DD)")
        })?
    } else {
        return Ok(vec![Message::Text(
            "언제의 시간표를 조회하시겠어요?".to_string(),
        )]);
    };

    if let Some(p) = period {
        if p.from.is_some() || p.to.is_some() {
            return Ok(vec![Message::Text(
                "여러날짜 조회를 지원하지 않습니다.".to_string(),
            )]);
        }
    }

    // 3) 데이터 로드
    let (_meal, _schedule, timetable) = preload_day_bundle(svc, date).await;

    let timetable_text = if let Some(t) = &timetable {
        build_timetable_text(date, grade, class, Some(t))
            .unwrap_or_else(|| "등록된 데이터가 없습니다.".to_string())
    } else {
        "등록된 데이터가 없습니다.".to_string()
    };

    if suggest_to_register && platform == "KT" {
        return Ok(vec![
            Message::Text(timetable_text),
            Message::Card(CardMessage {
                title: "방금 입력하신 정보를 저장할까요?".to_string(),
                description:
                    "학년/반 정보를 등록하시면 다음부터 더 빠르고 편하게 이용하실 수 있습니다."
                        .to_string(),
                thumbnail_url: None,
                buttons: vec![CardButton::Message {
                    title: "네, 저장해 주세요.".to_string(),
                    postback: Some(format!("사용자 정보 등록: {}학년 {}반", grade, class)),
                }],
            }),
        ]);
    }

    Ok(vec![Message::Text(timetable_text)])
}

fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

const ALLERGY_LABELS: &[&str] = &[
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

fn weekday_ko(date: NaiveDate) -> &'static str {
    match date.weekday() {
        Weekday::Mon => "월",
        Weekday::Tue => "화",
        Weekday::Wed => "수",
        Weekday::Thu => "목",
        Weekday::Fri => "금",
        Weekday::Sat => "토",
        Weekday::Sun => "일",
    }
}

fn briefing_schedule_text(date_label: &str, schedule: Option<&ScheduleDocument>) -> String {
    let summary = schedule
        .and_then(|s| s.summary.as_ref())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());
    match summary {
        Some(summary) => format!("{date_label} 학사일정:\n{summary}"),
        None => format!("{date_label}은 학사일정이 없습니다."),
    }
}

fn briefing_meal_text(
    date_label: &str,
    meal: Option<&MealDocument>,
    schedule: Option<&ScheduleDocument>,
    preferences: &UserPreferences,
) -> String {
    let Some(meal) = meal else {
        let schedule_text = schedule
            .and_then(|s| s.summary.as_ref())
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .unwrap_or("일정이 없습니다.");
        if schedule_text != "일정이 없습니다." {
            return format!("급식을 실시하지 않습니다. ({schedule_text})");
        }
        return "등록된 데이터가 없습니다.".to_string();
    };

    let pref = if preferences.allergy_info.is_empty() {
        "Number"
    } else {
        preferences.allergy_info.as_str()
    };

    let mut lines = Vec::new();
    for menu in &meal.menus {
        let clean_name = menu.name.replace('⭐', "").trim().to_string();
        let formatted = if pref == "None" || menu.allergies.is_empty() {
            clean_name
        } else if pref == "FullText" {
            let labels: Vec<String> = menu
                .allergies
                .iter()
                .filter_map(|a| ALLERGY_LABELS.get(*a as usize).copied())
                .map(|s| s.to_string())
                .collect();
            if labels.is_empty() {
                clean_name
            } else {
                format!("{clean_name}({})", labels.join(", "))
            }
        } else {
            let labels: Vec<String> = menu.allergies.iter().map(|a| a.to_string()).collect();
            if labels.is_empty() {
                clean_name
            } else {
                format!("{clean_name}({})", labels.join(", "))
            }
        };
        lines.push(formatted);
    }

    let mut text = format!("{date_label} 급식:\n{}", lines.join("\n"));
    text.push_str(&format!("\n\n열량: {} kcal", meal.calories.unwrap_or(0.0)));
    text
}

fn briefing_timetable_text(
    date_label: &str,
    date: NaiveDate,
    timetable: Option<&TimetableDocument>,
    grade: Option<i32>,
    class_no: Option<i32>,
) -> String {
    let Some(grade) = grade else {
        return "등록된 사용자만 시간표를 볼 수 있습니다.".to_string();
    };
    let Some(class_no) = class_no else {
        return "등록된 사용자만 시간표를 볼 수 있습니다.".to_string();
    };

    match build_timetable_text(date, grade, class_no, timetable) {
        Some(full_text) => {
            let body = full_text
                .split_once("):\n")
                .map(|(_, body)| body)
                .unwrap_or("");
            format!("{date_label} 시간표:\n{body}")
        }
        None => "등록된 시간표가 없습니다.".to_string(),
    }
}

fn build_timetable_text(
    date: NaiveDate,
    grade: i32,
    class_no: i32,
    timetable: Option<&TimetableDocument>,
) -> Option<String> {
    let timetable = timetable?;
    if timetable.lessons.is_empty() {
        return None;
    }
    let lessons = timetable
        .lessons
        .get(&grade.to_string())
        .and_then(|m| m.get(&class_no.to_string()))?;

    let mut text = format!(
        "{}학년 {}반,\n{}({}):",
        grade,
        class_no,
        date.format("%Y-%m-%d"),
        weekday_ko(date)
    );
    for (idx, subject) in lessons.iter().enumerate() {
        text.push_str(&format!("\n{}교시: {}", idx + 1, subject));
    }
    Some(text)
}
