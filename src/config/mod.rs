//! 환경변수 기반 설정.
//!
//! `.env` 파일을 [`dotenvy`] 로 먼저 로드한 뒤, [`AppConfig::from_env`] 가 모든
//! 필수 / 선택 변수를 파싱합니다. JSON 배열 / CSV 두 가지 포맷을 모두 허용하는
//! `parse_list` 헬퍼는 [`HDMeal_AuthTokens`], [`HDMeal_AllowedOrigins`] 에서
//! 사용됩니다.

use std::env;
use std::fmt;
use std::time::Duration;

use anyhow::{anyhow, Result as AnyhowResult};
use url::Url;

use crate::shared::security::hash_skill_token;

/// 글로벌 앱 설정. 한 번 로드되면 사실상 불변.
#[derive(Clone)]
pub struct AppConfig {
    pub app_name: String,
    pub debug: bool,
    pub port: u16,

    pub mongodb_uri: String,
    pub mongodb_database: String,

    pub neis_openapi_token: String,
    pub atpt_ofcdc_sc_code: String,
    pub sd_schul_code: String,
    pub num_of_grades: u32,
    pub num_of_classes: u32,

    pub kma_api_key: String,
    pub kma_nx: u32,
    pub kma_ny: u32,

    pub seoul_data_token: String,

    pub(crate) auth_token_hashes: Vec<[u8; 32]>,
    pub jwt_secret: String,
    pub base_url: Url,

    pub allowed_origins: Vec<String>,
    pub allow_credentials: bool,

    pub max_days_range: u32,
    pub app_version: String,
    pub app_build: u32,

    pub cache_health_timetable_ttl: Duration,
    pub cache_health_weather_ttl: Duration,
    pub cache_health_water_temp_ttl: Duration,

    pub otel_endpoint: Option<String>,
    pub otel_service_name: String,
}

impl fmt::Debug for AppConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppConfig")
            .field("app_name", &self.app_name)
            .field("debug", &self.debug)
            .field("port", &self.port)
            .field("mongodb_uri", &"<redacted>")
            .field("mongodb_database", &self.mongodb_database)
            .field("neis_openapi_token", &"<redacted>")
            .field("atpt_ofcdc_sc_code", &self.atpt_ofcdc_sc_code)
            .field("sd_schul_code", &self.sd_schul_code)
            .field("num_of_grades", &self.num_of_grades)
            .field("num_of_classes", &self.num_of_classes)
            .field("kma_api_key", &"<redacted>")
            .field("kma_nx", &self.kma_nx)
            .field("kma_ny", &self.kma_ny)
            .field("seoul_data_token", &"<redacted>")
            .field("auth_token_hashes", &"<redacted>")
            .field("jwt_secret", &"<redacted>")
            .field("base_url", &self.base_url)
            .field("allowed_origins", &self.allowed_origins)
            .field("allow_credentials", &self.allow_credentials)
            .field("max_days_range", &self.max_days_range)
            .field("app_version", &self.app_version)
            .field("app_build", &self.app_build)
            .field(
                "cache_health_timetable_ttl",
                &self.cache_health_timetable_ttl,
            )
            .field("cache_health_weather_ttl", &self.cache_health_weather_ttl)
            .field(
                "cache_health_water_temp_ttl",
                &self.cache_health_water_temp_ttl,
            )
            .field("otel_endpoint", &self.otel_endpoint)
            .field("otel_service_name", &self.otel_service_name)
            .finish()
    }
}

impl AppConfig {
    /// 환경에서 전체 설정을 로드합니다. `dotenvy` 가 자동으로 `.env` 를 찾습니다.
    ///
    /// Missing / malformed env vars 는 모두 `anyhow::Error` 로 bubble 됩니다 —
    /// 서버 startup 시점의 fatal error 이므로 [`crate::error::HDMealError`]
    /// (HTTP 4xx envelope) 로 표현하지 않습니다.
    pub fn from_env() -> AnyhowResult<Self> {
        let _ = dotenvy::dotenv();

        let app_name = env::var("APP_NAME").unwrap_or_else(|_| "hdmeal-backend".to_string());
        let debug = parse_bool("DEBUG").unwrap_or(false);
        let port = parse_u16("PORT").unwrap_or(8000);

        let mongodb_uri = required("MONGODB_URI")?;
        let mongodb_database = required("MONGODB_DATABASE")?;

        let neis_openapi_token = required("NEIS_OPENAPI_TOKEN")?;
        let atpt_ofcdc_sc_code = required("ATPT_OFCDC_SC_CODE")?;
        let sd_schul_code = required("SD_SCHUL_CODE")?;
        let num_of_grades =
            parse_u32("NUM_OF_GRADES").ok_or_else(|| anyhow!("NUM_OF_GRADES 가 필요합니다."))?;
        let num_of_classes =
            parse_u32("NUM_OF_CLASSES").ok_or_else(|| anyhow!("NUM_OF_CLASSES 가 필요합니다."))?;

        let kma_api_key = required("HDMeal_KMA_ApiKey")?;
        let kma_nx = parse_u32("HDMeal_KMA_NX").unwrap_or(60);
        let kma_ny = parse_u32("HDMeal_KMA_NY").unwrap_or(127);

        let seoul_data_token = required("HDMeal_SeoulData_Token")?;

        let auth_tokens = parse_list("HDMeal_AuthTokens")?;
        if auth_tokens.is_empty() {
            return Err(anyhow!("HDMeal_AuthTokens 가 필요합니다."));
        }
        let auth_token_hashes = auth_tokens
            .iter()
            .map(|token| hash_skill_token(token))
            .collect();
        let jwt_secret = required("HDMeal_JWTSecret")?.trim().to_string();
        if jwt_secret.is_empty() {
            return Err(anyhow!("HDMeal_JWTSecret 가 필요합니다."));
        }
        let base_url = Url::parse(&required("HDMeal_BaseURL")?)?;

        let mut allowed_origins = parse_list("HDMeal_AllowedOrigins").unwrap_or_default();
        if let Some(origin) = origin_from_url(&base_url) {
            if !allowed_origins.contains(&origin) {
                allowed_origins.push(origin);
            }
        }
        if debug {
            for dev in ["http://localhost:5173", "http://127.0.0.1:5173"] {
                if !allowed_origins.contains(&dev.to_string()) {
                    allowed_origins.push(dev.to_string());
                }
            }
        }
        if allowed_origins.iter().any(|o| o == "*") {
            allowed_origins = vec!["*".to_string()];
        }
        let allow_credentials = !allowed_origins.iter().any(|o| o == "*");

        let max_days_range = parse_u32("HDMeal_MaxDaysRange").unwrap_or(31);
        let app_version = env::var("HDMeal_AppVersion").unwrap_or_else(|_| "1.0.0".to_string());
        let app_build = parse_u32("HDMeal_AppBuild").unwrap_or(1);

        let cache_health_timetable_ttl =
            Duration::from_secs(parse_u64("CACHE_HEALTH_TIMETABLE_TTL_HOURS").unwrap_or(3) * 3600);
        let cache_health_weather_ttl =
            Duration::from_secs(parse_u64("CACHE_HEALTH_WEATHER_TTL_HOURS").unwrap_or(1) * 3600);
        let cache_health_water_temp_ttl = Duration::from_secs(
            parse_u64("CACHE_HEALTH_WATER_TEMP_TTL_MINUTES").unwrap_or(76) * 60,
        );

        let otel_endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let otel_service_name = env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| app_name.clone());

        Ok(Self {
            app_name,
            debug,
            port,
            mongodb_uri,
            mongodb_database,
            neis_openapi_token,
            atpt_ofcdc_sc_code,
            sd_schul_code,
            num_of_grades,
            num_of_classes,
            kma_api_key,
            kma_nx,
            kma_ny,
            seoul_data_token,
            auth_token_hashes,
            jwt_secret,
            base_url,
            allowed_origins,
            allow_credentials,
            max_days_range,
            app_version,
            app_build,
            cache_health_timetable_ttl,
            cache_health_weather_ttl,
            cache_health_water_temp_ttl,
            otel_endpoint,
            otel_service_name,
        })
    }
}

fn required(key: &str) -> AnyhowResult<String> {
    env::var(key).map_err(|_| anyhow!("{key} 가 필요합니다."))
}

fn parse_u16(key: &str) -> Option<u16> {
    env::var(key).ok()?.parse().ok()
}

fn parse_u32(key: &str) -> Option<u32> {
    env::var(key).ok()?.parse().ok()
}

fn parse_u64(key: &str) -> Option<u64> {
    env::var(key).ok()?.parse().ok()
}

fn parse_bool(key: &str) -> Option<bool> {
    std::env::var(key)
        .ok()
        .and_then(|v| match v.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Some(true),
            "false" | "0" | "no" | "off" | "" => Some(false),
            _ => None,
        })
}

/// JSON 배열 문자열을 우선 파싱, 실패하면 콤마 구분으로 분리.
pub fn parse_list(key: &str) -> AnyhowResult<Vec<String>> {
    let raw = match env::var(key) {
        Ok(v) => v,
        Err(_) => return Ok(Vec::new()),
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    if let Ok(arr) = serde_json::from_str::<Vec<String>>(trimmed) {
        return Ok(arr
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect());
    }
    Ok(trimmed
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

fn origin_from_url(url: &Url) -> Option<String> {
    let scheme = url.scheme();
    let host = url.host_str()?;
    let port = url.port();
    let mut origin = format!("{scheme}://{host}");
    if let Some(p) = port {
        let default_port = match scheme {
            "http" => 80,
            "https" => 443,
            _ => 0,
        };
        if p != default_port {
            origin.push_str(&format!(":{p}"));
        }
    }
    Some(origin)
}
