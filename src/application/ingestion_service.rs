//! IngestionService: NEIS 데이터 동기화.
//!
//! 두 가지 dedup 메커니즘:
//!
//! 1. **Cooldown** — 동일 키 (range 또는 `weather`, `water_temperature`) 가
//!    [`RANGE_COOLDOWN`] 이내에 성공한 경우 즉시 `None` 반환.
//! 2. **Singleflight** — 동시에 들어온 동일 키 요청은 하나의 fetch 만 실행하고
//!    나머지는 그 결과를 공유.
//!

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{NaiveDate, Utc};
use parking_lot::Mutex;
use tokio::sync::OnceCell;

use crate::infrastructure::neis::neis::NeisClient;
use crate::repository::DataService;

const RANGE_COOLDOWN: Duration = Duration::from_secs(10 * 60);
const RECENT_SYNC_TTL: Duration = Duration::from_secs(60 * 60);
const WEATHER_BACKGROUND_COOLDOWN: Duration = Duration::from_secs(30);
const WEATHER_BACKGROUND_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const SYNC_TIMEOUT_RANGE: Duration = Duration::from_secs(3);
const SYNC_TIMEOUT_AUX: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncStatus {
    Cooldown,
    Synced,
    Skipped,
    Background,
}

type SharedResult = Arc<Result<(), String>>;

struct InflightEntry {
    cell: OnceCell<SharedResult>,
}

pub struct IngestionService {
    data: Arc<DataService>,
    neis: Arc<NeisClient>,
    cooldown: Mutex<HashMap<String, Instant>>,
    inflight: Mutex<HashMap<String, Arc<InflightEntry>>>,
}

impl IngestionService {
    pub fn new(data: Arc<DataService>, neis: Arc<NeisClient>) -> Self {
        Self {
            data,
            neis,
            cooldown: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
        }
    }

    fn key_range(start: NaiveDate, end: NaiveDate) -> String {
        format!(
            "range:{}:{}",
            start.format("%Y-%m-%d"),
            end.format("%Y-%m-%d")
        )
    }
    fn key_weather() -> &'static str {
        "weather"
    }
    fn key_water() -> &'static str {
        "water_temperature"
    }

    fn check_cooldown(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut map = self.cooldown.lock();
        if let Some(last) = map.get(key) {
            if now.duration_since(*last) < RANGE_COOLDOWN {
                return true;
            }
        }
        prune_locked(now, &mut *map);
        false
    }

    fn mark_cooldown(&self, key: &str) {
        let now = Instant::now();
        let mut map = self.cooldown.lock();
        map.insert(key.to_string(), now);
    }

    /// 같은 키로 들어온 동시 요청을 묶어 한 번만 실행.
    /// `tokio::sync::OnceCell` 이 동시 호출자에게 동일한 future 를 공유합니다.
    async fn singleflight<F, Fut>(&self, key: &str, work: F) -> Result<(), String>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let entry = {
            let mut map = self.inflight.lock();
            map.entry(key.to_string())
                .or_insert_with(|| {
                    Arc::new(InflightEntry {
                        cell: OnceCell::new(),
                    })
                })
                .clone()
        };

        // OnceCell::get_or_init 은 init future 를 정확히 한 번만 실행한다.
        // 두 번째 호출자는 첫 번째의 완료를 기다린 후 동일한 값을 받는다.
        let shared: SharedResult = entry
            .cell
            .get_or_init(|| async move {
                let res = work().await;
                Arc::new(res)
            })
            .await
            .clone();

        // cleanup
        {
            let mut map = self.inflight.lock();
            if let Some(e) = map.get(key) {
                if Arc::ptr_eq(e, &entry) {
                    map.remove(key);
                }
            }
        }

        match shared.as_ref() {
            Ok(()) => Ok(()),
            Err(e) => Err(e.clone()),
        }
    }

    // --------- 공개 메서드 ---------

    /// 주어진 (start, end) 구간의 데이터를 동기화.
    /// cooldown 이면 즉시 Ok(()) (별도 표시 없이 통과), inflight 면 합류.
    pub async fn sync_range(&self, start: NaiveDate, end: NaiveDate) -> Result<SyncStatus, String> {
        let key = Self::key_range(start, end);
        if self.check_cooldown(&key) {
            return Ok(SyncStatus::Cooldown);
        }
        let data = self.data.clone();
        let neis = self.neis.clone();
        let key_for_mark = key.clone();
        let res = self
            .singleflight(&key, || async move {
                let fetched = neis
                    .fetch_all(start, end)
                    .await
                    .map_err(|e| e.to_string())?;
                for m in fetched.meals {
                    if let Err(e) = data.upsert_meal(&m).await {
                        tracing::warn!(error = %e, "upsert_meal failed");
                    }
                }
                for s in fetched.schedules {
                    if let Err(e) = data.upsert_schedule(&s).await {
                        tracing::warn!(error = %e, "upsert_schedule failed");
                    }
                }
                for t in fetched.timetables {
                    if let Err(e) = data.upsert_timetable(&t).await {
                        tracing::warn!(error = %e, "upsert_timetable failed");
                    }
                }
                Ok::<_, String>(())
            })
            .await;
        if res.is_ok() {
            self.mark_cooldown(&key_for_mark);
            Ok(SyncStatus::Synced)
        } else {
            res.map(|_| SyncStatus::Synced)
        }
    }

    /// 기본 윈도우: 어제 ~ +7 일.
    pub async fn sync_window(&self) -> Result<SyncStatus, String> {
        let today = crate::shared::timezone::today_kst_date();
        let start = today.pred_opt().unwrap_or(today);
        let end = today + chrono::Duration::days(7);
        self.sync_range(start, end).await
    }

    /// 오늘 ~ +N 일 구간을 동기화 대상으로 표시.
    pub async fn sync_window_offset(
        &self,
        past_days: i64,
        future_days: i64,
    ) -> Result<SyncStatus, String> {
        let today = crate::shared::timezone::today_kst_date();
        let start = today + chrono::Duration::days(-past_days);
        let end = today + chrono::Duration::days(future_days);
        self.sync_range(start, end).await
    }

    // ----- 챗봇 / ensure* -----

    /// 챗봇/핸들러에서 짧은 timeout 으로 range sync 시도.
    /// `Ok(true)` 는 동기화 완료, `Ok(false)` 는 timeout 또는 cooldown 으로 skip.
    pub async fn try_sync_range_short(&self, start: NaiveDate, end: NaiveDate) -> bool {
        match tokio::time::timeout(SYNC_TIMEOUT_RANGE, self.sync_range(start, end)).await {
            Ok(Ok(_)) => true,
            _ => false,
        }
    }

    /// 백그라운드 range sync spawn. 5분 timeout.
    pub fn spawn_background_range_sync(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> tokio::task::JoinHandle<()> {
        let me = self.clone_handles();
        tokio::spawn(async move {
            let _ =
                tokio::time::timeout(WEATHER_BACKGROUND_TIMEOUT, me.sync_range(start, end)).await;
        })
    }

    fn clone_handles(&self) -> IngestionHandle {
        IngestionHandle {
            data: self.data.clone(),
            neis: self.neis.clone(),
            cooldown: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
        }
    }
}

/// [`IngestionService`] 와 동일한 cooldown/singleflight 를 갖는 독립 핸들.
/// 백그라운드 spawn 전용으로, 메인 service 의 cooldown 상태와 격리됩니다.
struct IngestionHandle {
    data: Arc<DataService>,
    neis: Arc<NeisClient>,
    cooldown: Mutex<HashMap<String, Instant>>,
    inflight: Mutex<HashMap<String, Arc<InflightEntry>>>,
}

impl IngestionHandle {
    async fn sync_range(&self, start: NaiveDate, end: NaiveDate) -> Result<(), String> {
        // 단발성 백그라운드이므로 cooldown check 만 한다.
        let key = format!(
            "bg:range:{}:{}",
            start.format("%Y-%m-%d"),
            end.format("%Y-%m-%d")
        );
        {
            let now = Instant::now();
            let map = self.cooldown.lock();
            if let Some(last) = map.get(&key) {
                if now.duration_since(*last) < WEATHER_BACKGROUND_COOLDOWN {
                    return Ok(());
                }
            }
        }
        let res = async {
            let fetched = self
                .neis
                .fetch_all(start, end)
                .await
                .map_err(|e| e.to_string())?;
            for m in fetched.meals {
                if let Err(e) = self.data.upsert_meal(&m).await {
                    tracing::warn!(error = %e, "bg upsert_meal failed");
                }
            }
            for s in fetched.schedules {
                if let Err(e) = self.data.upsert_schedule(&s).await {
                    tracing::warn!(error = %e, "bg upsert_schedule failed");
                }
            }
            for t in fetched.timetables {
                if let Err(e) = self.data.upsert_timetable(&t).await {
                    tracing::warn!(error = %e, "bg upsert_timetable failed");
                }
            }
            Ok::<_, String>(())
        }
        .await;
        if res.is_ok() {
            self.cooldown.lock().insert(key, Instant::now());
        }
        res
    }
}

/// 챗봇 등에서 쓰는 짧은 timeout sync.
pub async fn with_short_timeout<F, T>(timeout: Duration, fut: F) -> Option<T>
where
    F: std::future::Future<Output = T>,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(v) => Some(v),
        Err(_) => None,
    }
}

fn prune_locked(now: Instant, map: &mut HashMap<String, Instant>) {
    map.retain(|_, t| now.duration_since(*t) < RECENT_SYNC_TTL);
}

pub const SYNC_TIMEOUT_RANGE_DURATION: Duration = SYNC_TIMEOUT_RANGE;
pub const SYNC_TIMEOUT_AUX_DURATION: Duration = SYNC_TIMEOUT_AUX;

/// 챗봇 캐시 freshness 헬퍼: `created_at` 기준 TTL 비교.
pub fn is_fresh(created_at: chrono::DateTime<Utc>, ttl: Duration) -> bool {
    let now = Utc::now();
    let age = now.signed_duration_since(created_at);
    age <= chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::seconds(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cooldown_returns_true_within_window() {
        // 실제 service 가 필요한 테스트는 통합 테스트에서 다룬다. 여기서는 헬퍼.
        assert!(is_fresh(
            Utc::now() - chrono::Duration::seconds(5),
            Duration::from_secs(60),
        ));
        assert!(!is_fresh(
            Utc::now() - chrono::Duration::seconds(120),
            Duration::from_secs(60),
        ));
    }
}
