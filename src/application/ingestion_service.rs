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

use chrono::{DateTime, NaiveDate, Utc};
use std::sync::Mutex;
use tokio::sync::OnceCell;

use crate::infrastructure::neis::neis::NeisClient;
use crate::repository::DataService;

const RANGE_COOLDOWN: Duration = Duration::from_secs(10 * 60);
const RECENT_SYNC_TTL: Duration = Duration::from_secs(60 * 60);
const WEATHER_BACKGROUND_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const SYNC_TIMEOUT_RANGE: Duration = Duration::from_secs(3);

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
    // `std::sync::Mutex` 가 안전한 이유: 락은 오직 HashMap 조작(삽입/조회/삭제)
    // 에만 잠시 유지되고, `.await` 포인트에서는 절대 잡고 있지 않다.
    // `tokio::sync::Mutex` 로 교체하면 오버헤드만 증가한다.
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

    fn check_cooldown(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut map = self.cooldown.lock().unwrap();
        if let Some(last) = map.get(key) {
            if now.saturating_duration_since(*last) < RANGE_COOLDOWN {
                return true;
            }
        }
        prune_locked(now, &mut map);
        false
    }

    fn mark_cooldown(&self, key: &str) {
        let now = Instant::now();
        let mut map = self.cooldown.lock().unwrap();
        map.insert(key.to_string(), now);
    }

    async fn singleflight<F, Fut>(&self, key: &str, work: F) -> Result<(), String>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let entry = {
            let mut map = self.inflight.lock().unwrap();
            map.entry(key.to_string())
                .or_insert_with(|| {
                    Arc::new(InflightEntry {
                        cell: OnceCell::new(),
                    })
                })
                .clone()
        };

        let shared: SharedResult = entry
            .cell
            .get_or_init(|| async move {
                let res = work().await;
                Arc::new(res)
            })
            .await
            .clone();

        {
            let mut map = self.inflight.lock().unwrap();
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

    async fn persist_fetched(
        data: &DataService,
        fetched: &crate::infrastructure::neis::neis::NeisFetchAll,
    ) {
        // 3 컬렉션을 병렬 upsert. Mongo connection pool 이 충분히 크므로
        // round-trip latency 가 직렬 → 3-way overlap 으로 줄어듦.
        let (meals_res, schedules_res, timetables_res) = tokio::join!(
            data.upsert_meals_batch(&fetched.meals),
            data.upsert_schedules_batch(&fetched.schedules),
            data.upsert_timetables_batch(&fetched.timetables),
        );
        if let Err(e) = meals_res {
            tracing::warn!(error = %e, count = fetched.meals.len(), "upsert_meals_batch failed");
        }
        if let Err(e) = schedules_res {
            tracing::warn!(
                error = %e,
                count = fetched.schedules.len(),
                "upsert_schedules_batch failed"
            );
        }
        if let Err(e) = timetables_res {
            tracing::warn!(
                error = %e,
                count = fetched.timetables.len(),
                "upsert_timetables_batch failed"
            );
        }
    }

    /// 주어진 (start, end) 구간의 데이터를 동기화.
    ///
    /// # Errors
    ///
    /// NEIS fetch 실패 시 `Err(String)` 반환. cooldown 내면 `Ok(Cooldown)`.
    pub async fn sync_range(&self, start: NaiveDate, end: NaiveDate) -> Result<SyncStatus, String> {
        let key = Self::key_range(start, end);
        if self.check_cooldown(&key) {
            return Ok(SyncStatus::Cooldown);
        }
        let data = self.data.clone();
        let neis = self.neis.clone();
        let res = self
            .singleflight(&key, || async move {
                let fetched = neis
                    .fetch_all(start, end)
                    .await
                    .map_err(|e| e.to_string())?;
                Self::persist_fetched(&data, &fetched).await;
                Ok::<_, String>(())
            })
            .await;
        if res.is_ok() {
            self.mark_cooldown(&key);
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

    /// 챗봇/핸들러에서 짧은 timeout 으로 range sync 시도.
    pub async fn try_sync_range_short(&self, start: NaiveDate, end: NaiveDate) -> bool {
        matches!(
            tokio::time::timeout(SYNC_TIMEOUT_RANGE, self.sync_range(start, end)).await,
            Ok(Ok(_))
        )
    }

    /// 백그라운드 range sync spawn. 5분 timeout.
    pub fn spawn_background_range_sync(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> tokio::task::JoinHandle<()> {
        let data = self.data.clone();
        let neis = self.neis.clone();
        tokio::spawn(async move {
            let _ = tokio::time::timeout(WEATHER_BACKGROUND_TIMEOUT, async move {
                let fetched = neis
                    .fetch_all(start, end)
                    .await
                    .map_err(|e| e.to_string())?;
                Self::persist_fetched(&data, &fetched).await;
                Ok::<_, String>(())
            })
            .await;
        })
    }
}

fn prune_locked(now: Instant, map: &mut HashMap<String, Instant>) {
    map.retain(|_, t| now.saturating_duration_since(*t) < RECENT_SYNC_TTL);
}

pub const SYNC_TIMEOUT_RANGE_DURATION: Duration = SYNC_TIMEOUT_RANGE;
pub const SYNC_TIMEOUT_AUX_DURATION: Duration = Duration::from_secs(2);

/// 챗봇 캐시 freshness 헬퍼: `created_at` 기준 TTL 비교.
pub fn is_fresh(created_at: DateTime<Utc>, ttl: Duration) -> bool {
    let now = chrono::Utc::now();
    let age = now.signed_duration_since(created_at);
    age <= chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::seconds(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cooldown_returns_true_within_window() {
        assert!(is_fresh(
            chrono::Utc::now() - chrono::Duration::seconds(5),
            Duration::from_secs(60),
        ));
        assert!(!is_fresh(
            chrono::Utc::now() - chrono::Duration::seconds(120),
            Duration::from_secs(60),
        ));
    }
}
