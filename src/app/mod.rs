//! 컴포지션 루트: 모든 의존성을 와이어업하고 tokio 런타임에서 HTTP 서버를 띄움.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::signal;

use crate::application::AppContext;
use crate::config::AppConfig;
use crate::infrastructure::neis::auxiliary::{KmaClient, SeoulWaterClient};
use crate::infrastructure::neis::http_client::HttpClient;
use crate::infrastructure::neis::neis::NeisClient;
use crate::repository::DataService;
use crate::scheduler::PeriodicTask;
use crate::shared::observability;
use crate::transport::http;

/// 모든 초기화 + 서버 시작.
pub async fn run() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;
    observability::init(&config.app_name, config.otel_endpoint.as_deref())?;

    // Repository (MongoDB)
    let data = Arc::new(DataService::new(&config).await?);
    tracing::info!(db = %config.mongodb_database, "MongoDB connected");

    // Infrastructure
    let http = HttpClient::new()?;
    let neis = Arc::new(NeisClient::new(config.clone(), http.clone()));
    let kma = Arc::new(KmaClient::new(config.clone(), http.clone()));
    let seoul_water = Arc::new(SeoulWaterClient::new(
        http.clone(),
        config.seoul_data_token.clone(),
    ));

    // AppContext
    let config = Arc::new(config);
    let ctx = Arc::new(AppContext::new(
        data.clone(),
        neis.clone(),
        kma,
        seoul_water,
        config.clone(),
    ));

    // 시작 시: today-1 ~ today+10 미리 sync (Go 와 동일)
    {
        let neis = neis.clone();
        let data = data.clone();
        let start = crate::shared::timezone::today_kst_date() - chrono::Duration::days(1);
        let end = crate::shared::timezone::today_kst_date() + chrono::Duration::days(10);
        let res = tokio::time::timeout(Duration::from_secs(60), async move {
            match neis.fetch_all(start, end).await {
                Ok(fetched) => {
                    for m in fetched.meals {
                        let _ = data.upsert_meal(&m).await;
                    }
                    for s in fetched.schedules {
                        let _ = data.upsert_schedule(&s).await;
                    }
                    for t in fetched.timetables {
                        let _ = data.upsert_timetable(&t).await;
                    }
                }
                Err(e) => tracing::warn!(error = %e, "initial warmup failed"),
            }
        })
        .await;
        if res.is_err() {
            tracing::warn!("initial warmup timed out");
        }
    }

    // 3 시간 주기 sync_window
    let periodic = PeriodicTask::new(Duration::from_secs(3 * 60 * 60));
    let ingestion = ctx.ingestion.clone();
    periodic.start(move || {
        let i = ingestion.clone();
        async move {
            if let Err(e) = i.sync_window().await {
                tracing::warn!(error = %e, "periodic window sync failed");
            }
        }
    });

    // HTTP 서버
    let router = http::build_router(config.clone(), ctx.clone());
    let addr: SocketAddr = ([0, 0, 0, 0], config.port).into();
    tracing::info!(%addr, "HTTP server listening");

    let server_fut = warp::serve(router).run(addr);
    let server_handle = server_fut;
    let shutdown = async {
        if let Err(e) = signal::ctrl_c().await {
            tracing::error!(error = %e, "ctrl_c failed");
        }
        tracing::info!("shutdown signal received");
    };
    tokio::select! {
        _ = server_handle => {},
        _ = shutdown => {
            periodic.stop().await;
            crate::repository::close().await;
            observability::shutdown();
        }
    }

    Ok(())
}
