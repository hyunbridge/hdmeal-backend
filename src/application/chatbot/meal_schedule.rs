//! Meal / Schedule intent 핸들러.

use chrono::{Datelike, NaiveDate, Weekday};

use crate::application::chatbot::types::{KakaoSkillRequest, Message};
use crate::application::chatbot::Service;
use crate::error::HDMealResult;
use crate::shared::timezone::{format_date_label, today_kst_date};

use super::sync_helpers::preload_day_bundle;

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

/// Meal: 단일 날짜의 중식.
pub async fn handle_meal(
    svc: &Service,
    req: &KakaoSkillRequest,
    platform: &str,
    external_id: &str,
) -> HDMealResult<Vec<Message>> {
    let date_param = req.action.get_date();
    let period = req.action.get_date_period();

    // date_period 가 주어지면 거부
    if let Some(p) = period {
        if p.from.is_some() || p.to.is_some() {
            return Ok(vec![Message::Text(
                "현재 식단조회에서는 여러날짜 조회를 지원하지 않습니다.".to_string(),
            )]);
        }
    }

    let date = match date_param {
        Some(d) => match parse_date(&d) {
            Some(d) => d,
            None => {
                return Ok(vec![Message::Text(
                    "잘못된 날짜 형식입니다. (YYYY-MM-DD)".to_string(),
                )]);
            }
        },
        None => {
            return Ok(vec![Message::Text(
                "언제의 급식을 조회하시겠어요?".to_string(),
            )]);
        }
    };

    let weekday = date.weekday();
    if matches!(weekday, Weekday::Sat | Weekday::Sun) {
        return Ok(vec![Message::Text(
            "급식을 실시하지 않습니다. (주말)".to_string(),
        )]);
    }

    let (meal, schedule, _timetable) = preload_day_bundle(svc, date).await;
    if meal.is_none() {
        let schedule_text = schedule
            .as_ref()
            .and_then(|s| s.summary.as_ref())
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_default();
        if !schedule_text.is_empty() {
            return Ok(vec![Message::Text(format!(
                "급식을 실시하지 않습니다. ({})",
                schedule_text
            ))]);
        }
        return Ok(vec![Message::Text("등록된 데이터가 없습니다.".to_string())]);
    }
    let meal = meal.unwrap();

    // AllergyInfo preference 조회
    let user = svc.users.ensure_user(platform, external_id).await?;
    let mode = if user.preferences.allergy_info.is_empty() {
        "Number".to_string()
    } else {
        user.preferences.allergy_info
    };

    let mut text = format!("{}:\n", format_date_label(date));
    let mut menus = Vec::new();
    for menu in &meal.menus {
        let clean_name = menu.name.replace('⭐', "").trim().to_string();
        let label = match mode.as_str() {
            "None" => clean_name,
            "FullText" => {
                let names: Vec<String> = menu
                    .allergies
                    .iter()
                    .filter_map(|&a| ALLERGY_LABELS.get(a as usize).copied())
                    .map(String::from)
                    .collect();
                if names.is_empty() {
                    clean_name
                } else {
                    format!("{}({})", clean_name, names.join(", "))
                }
            }
            _ => {
                if menu.allergies.is_empty() {
                    clean_name
                } else {
                    format!(
                        "{}({})",
                        clean_name,
                        menu.allergies
                            .iter()
                            .map(|a| a.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            }
        };
        menus.push(label);
    }
    text.push_str(&menus.join("\n"));
    text.push_str(&format!("\n\n열량: {} kcal", meal.calories.unwrap_or(0.0)));
    Ok(vec![Message::Text(text)])
}

/// Schedule: 단일 날짜 또는 날짜 범위 일정.
/// 90일 초과면 end 를 start+90 으로 제한.
pub async fn handle_schedule(
    svc: &Service,
    req: &KakaoSkillRequest,
    _platform: &str,
    _external_id: &str,
) -> HDMealResult<Vec<Message>> {
    let date_param = req.action.get_date();
    let period = req.action.get_date_period();

    // 단일 날짜
    if let Some(d) = date_param {
        if period.is_some() {
            return Ok(vec![Message::Text(
                "여러날짜 조회를 지원하지 않습니다.".to_string(),
            )]);
        }
        let date = match parse_date(&d) {
            Some(d) => d,
            None => {
                return Ok(vec![Message::Text(
                    "잘못된 날짜 형식입니다. (YYYY-MM-DD)".to_string(),
                )]);
            }
        };
        let (start, end) = (date, date);
        let _ = svc.ingestion.try_sync_range_short(start, end).await;
        let docs = svc
            .data
            .get_schedules_in_range(
                &start.format("%Y-%m-%d").to_string(),
                &end.format("%Y-%m-%d").to_string(),
            )
            .await
            .unwrap_or_default();
        let body = if let Some(doc) = docs.first() {
            doc.summary
                .as_ref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|summary| format!("{}:\n{}", format_date_label(start), summary))
                .unwrap_or_else(|| "일정이 없습니다.".to_string())
        } else {
            "일정이 없습니다.".to_string()
        };
        return Ok(vec![Message::Text(body)]);
    }

    if let Some(p) = period {
        let from = p
            .from
            .as_deref()
            .and_then(parse_date)
            .unwrap_or_else(today_kst_date);
        let to =
            p.to.as_deref()
                .and_then(parse_date)
                .unwrap_or(from + chrono::Duration::days(7));
        if from > to {
            return Ok(vec![Message::Text(
                "시작일이 종료일보다 늦습니다.".to_string(),
            )]);
        }
        let effective_end = if (to - from).num_days() > 90 {
            from + chrono::Duration::days(90)
        } else {
            to
        };
        let notice = if (to - from).num_days() > 90 {
            format!(
                "서버 성능상의 이유로 최대 90일까지만 조회가 가능합니다.\n조회기간이 {}부터 {}까지로 제한되었습니다.\n\n",
                from,
                effective_end
            )
        } else {
            format!("{}부터 {}까지 조회합니다.\n\n", from, effective_end)
        };
        let _ = svc
            .ingestion
            .try_sync_range_short(from, effective_end)
            .await;
        let docs = svc
            .data
            .get_schedules_in_range(
                &from.format("%Y-%m-%d").to_string(),
                &effective_end.format("%Y-%m-%d").to_string(),
            )
            .await
            .unwrap_or_default();
        let text = render_schedule_range(&docs, from, effective_end, false);
        let text = text.trim_end_matches('\n').to_string();
        return Ok(vec![Message::Text(format!("{notice}{text}"))]);
    }

    Ok(vec![Message::Text(
        "언제의 일정을 조회하시겠어요?".to_string(),
    )])
}

/// docs 를 `start..=end` 범위에서 같은 summary 가 연속되면 묶어서 출력.
/// `single` 이면 항상 한 줄 (단일 날짜 형식).
fn render_schedule_range(
    docs: &[crate::domain::ScheduleDocument],
    start: NaiveDate,
    end: NaiveDate,
    _single: bool,
) -> String {
    use std::collections::BTreeMap;
    let mut by_date: BTreeMap<String, String> = BTreeMap::new();
    for d in docs {
        let s = d.summary.clone().unwrap_or_default();
        by_date.insert(d.date.clone(), s);
    }

    if start == end {
        let key = start.format("%Y-%m-%d").to_string();
        let s = by_date
            .get(&key)
            .cloned()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "일정이 없습니다.".to_string());
        if s.is_empty() {
            return format!("{}:\n일정이 없습니다.", format_date_label(start));
        }
        return format!("{}:\n{}", format_date_label(start), s);
    }

    // 같은 summary 의 연속 구간을 묶음.
    let mut current_summary: Option<String> = None;
    let mut group_start: Option<NaiveDate> = None;
    let mut group_end: Option<NaiveDate> = None;
    let mut out = String::new();

    let mut d = start;
    while d <= end {
        let key = d.format("%Y-%m-%d").to_string();
        let s = by_date
            .get(&key)
            .cloned()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "일정이 없습니다.".to_string());
        match &current_summary {
            Some(cur) if cur == &s => {
                group_end = Some(d);
            }
            _ => {
                if let (Some(gs), Some(ge), Some(cs)) =
                    (group_start, group_end, current_summary.take())
                {
                    out.push_str(&format_group_line(gs, ge, &cs));
                }
                current_summary = Some(s);
                group_start = Some(d);
                group_end = Some(d);
            }
        }
        d = d.succ_opt().unwrap_or(d);
    }
    if let (Some(gs), Some(ge), Some(cs)) = (group_start, group_end, current_summary) {
        out.push_str(&format_group_line(gs, ge, &cs));
    }
    if out.is_empty() {
        return "일정이 없습니다.".to_string();
    }
    out
}

fn format_group_line(start: NaiveDate, end: NaiveDate, summary: &str) -> String {
    if start == end {
        format!("{}:\n{}\n", format_date_label(start), summary)
    } else {
        format!(
            "{}~{}:\n{}\n",
            format_date_label(start),
            format_date_label(end),
            summary
        )
    }
}

fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}
