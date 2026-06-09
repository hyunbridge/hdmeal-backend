//! Meal / Schedule intent 핸들러.

use chrono::{Datelike, NaiveDate, Weekday};

use crate::application::chatbot::types::{
    format_menu_with_allergies, parse_date, KakaoSkillRequest, Message,
};
use crate::application::chatbot::Service;
use crate::error::HDMealResult;
use crate::shared::timezone::{format_date_label, today_kst_date};

use super::sync_helpers::preload_day_bundle;

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
    let Some(meal) = meal else {
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
    };

    // AllergyInfo preference 조회
    let user = svc.users.ensure_user(platform, external_id).await?;
    let mode = if user.preferences.is_empty() {
        "Number"
    } else {
        user.preferences
            .get("AllergyInfo")
            .map(|s| s.as_str())
            .unwrap_or("Number")
    };

    let mut text = format!("{}:\n", format_date_label(date));
    let mut menus = Vec::new();
    for menu in &meal.menus {
        menus.push(format_menu_with_allergies(
            &menu.name,
            &menu.allergies,
            mode,
        ));
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
        let Some(next) = d.succ_opt() else {
            break;
        };
        d = next;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ScheduleDocument;
    use chrono::Utc;

    fn d(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn render_single_date_with_schedule() {
        let docs = vec![ScheduleDocument {
            id: String::new(),
            date: "2026-06-09".to_string(),
            entries: vec![],
            summary: Some("중간고사".to_string()),
            created_at: Utc::now(),
        }];
        let result = render_schedule_range(&docs, d(2026, 6, 9), d(2026, 6, 9), false);
        assert!(result.contains("중간고사"));
    }

    #[test]
    fn render_single_date_no_schedule() {
        let result = render_schedule_range(&[], d(2026, 6, 9), d(2026, 6, 9), false);
        assert!(result.contains("일정이 없습니다"));
    }

    #[test]
    fn render_range_groups_same_summary() {
        let docs = vec![
            ScheduleDocument {
                id: String::new(),
                date: "2026-06-09".to_string(),
                entries: vec![],
                summary: Some("방학".to_string()),
                created_at: Utc::now(),
            },
            ScheduleDocument {
                id: String::new(),
                date: "2026-06-10".to_string(),
                entries: vec![],
                summary: Some("방학".to_string()),
                created_at: Utc::now(),
            },
            ScheduleDocument {
                id: String::new(),
                date: "2026-06-11".to_string(),
                entries: vec![],
                summary: Some("개학".to_string()),
                created_at: Utc::now(),
            },
        ];
        let result = render_schedule_range(&docs, d(2026, 6, 9), d(2026, 6, 11), false);
        assert!(result.contains("방학"));
        assert!(result.contains("개학"));
        assert!(
            result.contains('~'),
            "should contain a date range: {result}"
        );
    }

    #[test]
    fn render_range_with_gaps() {
        let docs = vec![ScheduleDocument {
            id: String::new(),
            date: "2026-06-11".to_string(),
            entries: vec![],
            summary: Some("개학".to_string()),
            created_at: Utc::now(),
        }];
        let result = render_schedule_range(&docs, d(2026, 6, 9), d(2026, 6, 11), false);
        assert!(result.contains("일정이 없습니다"));
        assert!(result.contains("개학"));
    }

    #[test]
    fn format_group_line_same_date() {
        let line = format_group_line(d(2026, 6, 9), d(2026, 6, 9), "테스트");
        assert!(!line.contains('~'));
        assert!(line.contains("테스트"));
    }

    #[test]
    fn format_group_line_date_range() {
        let line = format_group_line(d(2026, 6, 9), d(2026, 6, 11), "테스트");
        assert!(line.contains('~'));
        assert!(line.contains("테스트"));
    }

    #[test]
    fn render_empty_range() {
        let result = render_schedule_range(&[], d(2026, 6, 9), d(2026, 6, 9), false);
        assert!(result.contains("일정이 없습니다"));
    }

    #[test]
    fn render_schedule_with_empty_summary_falls_back() {
        let docs = vec![ScheduleDocument {
            id: String::new(),
            date: "2026-06-09".to_string(),
            entries: vec![],
            summary: Some("".to_string()),
            created_at: Utc::now(),
        }];
        let result = render_schedule_range(&docs, d(2026, 6, 9), d(2026, 6, 9), false);
        assert!(result.contains("일정이 없습니다"));
    }

    #[test]
    fn render_schedule_with_none_summary_falls_back() {
        let docs = vec![ScheduleDocument {
            id: String::new(),
            date: "2026-06-09".to_string(),
            entries: vec![],
            summary: None,
            created_at: Utc::now(),
        }];
        let result = render_schedule_range(&docs, d(2026, 6, 9), d(2026, 6, 9), false);
        assert!(result.contains("일정이 없습니다"));
    }
}
