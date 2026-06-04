# HDMeal Backend

레거시 Flask 서버([hdmeal-chatbot](https://github.com/hyunbridge/hdmeal-chatbot), [hdmeal-json](https://github.com/hyunbridge/hdmeal-json))를 대체하기 위해 작성된 흥덕고 급식봇/모바일 앱용 통합 백엔드입니다.

## 기능

- 모바일 앱용 통합 데이터 API(급식/학사일정/시간표): `/api/app/*`
- 카카오 i 오픈빌더 Skill 엔드포인트: `/skill/`
- 사용자 설정(학년/반, 알레르기 표기 등): `/user/settings/`
- 캐시 상태 점검: `/cache/healthcheck/`
- 운영 프로브(k8s liveness/readiness) 및 Prometheus 메트릭: `/livez`, `/readyz`, `/metrics`
- 주기적 데이터 동기화(NEIS + 보조 API, singleflight + 10분 쿨다운) 및 MongoDB 캐싱

## 기술 스택

- Rust 1.75+ (Edition 2021)
- Warp 0.3 + Tokio (multi-thread runtime)
- MongoDB 공식 드라이버 3.x (async, `rustls-tls`)
- Serde / BSON / Chrono
- `jsonwebtoken` 9 (HS256, RustCrypto — `aws-lc-rs` 미사용)
- `reqwest` (`rustls-tls`, gzip)
- OpenTelemetry (OTLP/gRPC, W3C Trace Context)

## 프로젝트 구조

```
hdmeal-backend/
├── Cargo.toml
├── Dockerfile
├── data/delicious.txt          # NEIS 메뉴에 ⭐ 마킹할 키워드
├── src/
│   ├── main.rs                 # 엔트리포인트
│   ├── lib.rs                  # 모듈 노출
│   ├── app/                    # 컴포지션 루트 (run())
│   ├── config/                 # env 기반 설정 (AppConfig)
│   ├── domain/                 # 도메인 / 직렬화 모델
│   ├── repository/             # MongoDB CRUD (DataService)
│   ├── application/
│   │   ├── ingestion_service.rs # singleflight + 10분 쿨다운
│   │   ├── user_service.rs
│   │   └── chatbot/            # 카카오 챗봇 intent 디스패치
│   ├── infrastructure/
│   │   └── neis/               # NEIS / KMA / Seoul Open Data
│   ├── scheduler/              # 3h 주기 periodic task
│   ├── shared/                 # base58, JWT, UUIDv7, KST, observability
│   ├── transport/http/         # Warp 라우터 + DTO
│   └── error.rs                # HDMealError + Warp reject 변환
└── tests/                      # 통합/단위 테스트
```

## 데이터 흐름(캐시)

- 요청 처리 시 `IngestionService`가(singleflight + 10분 쿨다운) 조회 구간을 먼저 동기화한 뒤, MongoDB 캐시에서 데이터를 읽어 응답합니다.
- 앱 시작 시 10일 전~10일 후 구간을 워밍업하고, 이후 3시간 간격으로 주기 동기화를 수행합니다.
- MongoDB 컬렉션: `meals`, `schedules`, `timetables`, `weather`, `water_temperatures`, `users`

## 인증/권한

- `X-Request-ID`: 요청 추적용 표준 헤더(서버는 기존 `X-HDMeal-Req-ID`도 호환 입력으로 수용)
- `traceparent` / `tracestate`: OpenTelemetry W3C Trace Context 전파
- `X-HDMeal-Token`: 카카오 스킬 호출 인증(서버가 보유한 `HDMeal_AuthTokens`와 비교). `?token=` / `Authorization: Bearer …` 형식도 지원.
- 사용자 설정 API: `X-HDMeal-Token`에 JWT를 사용하며 scope 기반으로 권한을 검사합니다.
- 요청 추적 ID는 UUIDv7 형식으로 생성됩니다.

## 주요 Endpoint

| Method | Path | Auth |
|---|---|---|
| GET | `/healthz` | – |
| GET | `/livez` | – (k8s liveness — 프로세스 생존) |
| GET | `/readyz` | – (k8s readiness — Mongo ping 성공 시 200, 실패 시 503) |
| GET | `/metrics` | – (Prometheus text format) |
| GET | `/api/app/days?from=YYYY-MM-DD&to=YYYY-MM-DD` | – |
| GET | `/api/app/days/{YYYY-MM-DD}` | – |
| GET | `/api/app/meta` | – |
| POST | `/skill/` | `X-HDMeal-Token` (또는 `?token=` / `Authorization: Bearer …`) |
| GET | `/user/settings/` | JWT (`GetUserInfo`) |
| PATCH | `/user/settings/` | JWT (`ManageUserInfo`) |
| DELETE | `/user/settings/` | JWT (`ManageUserInfo`) |
| GET | `/cache/healthcheck/` | – |

### 응답 헤더

- `X-Request-ID` — UUIDv7 (클라이언트에서 보낸 값이 UUIDv7이면 그대로 사용)
- `X-HDMeal-Req-ID`, `X-HDMeal-ReqId` — legacy 호환
- `traceparent`, `tracestate` — OTel W3C Trace Context
- `X-HDMeal-Range` — `/api/app/days*` 응답에만 포함
- 보안 헤더: `Strict-Transport-Security`, `X-Content-Type-Options`, `X-Frame-Options`, `Content-Security-Policy`, `Referrer-Policy`

### 에러 응답

```json
{ "detail": "<한국어 메시지>", "requestId": "<uuidv7>" }
```

### 운영 엔드포인트

- `GET /healthz` — 단순 liveness. 핸들러 본체만 검증. 외부 의존성 무관.
- `GET /livez` — k8s `livenessProbe` 용. 프로세스가 살아있으면 200.
- `GET /readyz` — k8s `readinessProbe` 용. Mongo `ping` 성공 시 200, 실패 시 503. 트래픽 분기 결정.
- `GET /metrics` — Prometheus 텍스트 포맷. `http_requests_total{path,method,status}`,
  `process_start_time_seconds` 노출.

```yaml
# k8s probe 예시
livenessProbe:
  httpGet: { path: /livez, port: 8000 }
  periodSeconds: 10
readinessProbe:
  httpGet: { path: /readyz, port: 8000 }
  periodSeconds: 5
  failureThreshold: 3
```

```promql
# Prometheus query 예시
rate(http_requests_total{status=~"5.."}[5m])
sum by (path) (rate(http_requests_total[1m]))
```

### 챗봇 intents

`Briefing`, `Meal`, `Timetable`, `Schedule`, `WaterTemperature`, `UserSettings`, `ModifyUserInfo`, `Unknown`

## 로컬 개발

```bash
cp .env.example .env
# .env 수정 후
cargo run --release
# 또는 Docker
docker build -t hdmeal-backend .
docker run --rm -p 8000:8000 --env-file .env hdmeal-backend
```

## 환경 변수

런타임 필수·선택 값은 `.env.example`에 정의되어 있습니다.

### 필수(런타임)

- `MONGODB_URI`, `MONGODB_DATABASE`: MongoDB 연결 정보
- `NEIS_OPENAPI_TOKEN`, `ATPT_OFCDC_SC_CODE`, `SD_SCHUL_CODE`: NEIS OpenAPI 인증/학교 식별자
- `NUM_OF_GRADES`, `NUM_OF_CLASSES`: 시간표/사용자 설정 범위(학년/반). 모바일/웹 클라이언트와 반드시 일치해야 함.
- `HDMeal_AuthTokens`: 카카오 스킬 인증 토큰 (JSON 배열 또는 CSV)
- `HDMeal_JWTSecret`: 사용자 설정 JWT 서명 키
- `HDMeal_SeoulData_Token`: 서울 열린데이터(한강 수온) API 키
- `HDMeal_KMA_ApiKey`: KMA 동네예보 API 키 (URL Encode되지 않은 값)
- `HDMeal_KMA_NX`, `HDMeal_KMA_NY`: KMA 동네예보 격자 좌표 (기본 62, 120)
- `HDMeal_BaseURL`: 사용자 설정 웹 베이스 URL (카드 링크/Allowed Origins 계산에 사용)

### 선택(런타임)

- `PORT`: 서버 포트 (기본 8000)
- `APP_NAME`: 앱 식별자 (기본 `hdmeal-backend`)
- `DEBUG`: 디버그 모드 (기본 `false`)
- `HDMeal_AllowedOrigins`: CORS 허용 Origin (JSON 배열 또는 CSV, `"*"` 지원). 미설정 시 개발용 origin이 자동 포함.
- `HDMeal_MaxDaysRange`: `/api/app/days` 최대 조회 범위 (기본 31일)
- `HDMeal_AppVersion`, `HDMeal_AppBuild`: `/api/app/meta` 응답 값
- `CACHE_HEALTH_TIMETABLE_TTL_HOURS`, `CACHE_HEALTH_WEATHER_TTL_HOURS`, `CACHE_HEALTH_WATER_TEMP_TTL_MINUTES`: 캐시 헬스 TTL

### 관측성(OTel)

- `OTEL_EXPORTER_OTLP_ENDPOINT`: gRPC endpoint (예: `http://otel-collector:4317`). 비우면 OTel 비활성.
- `OTEL_SERVICE_NAME`: `service.name` resource attribute (기본 `APP_NAME`)
- `RUST_LOG`: `tracing-subscriber` EnvFilter. 기본 `info,hdmeal_backend=debug`

## 테스트

```bash
cargo test
```

## OpenTelemetry

`OTEL_EXPORTER_OTLP_ENDPOINT`가 비어 있으면 OTel 트레이싱은 **비활성**됩니다 (콘솔 로그만). 채우면 자동 활성화.

### 활성화

```bash
# 로컬 — jaeger-all-in-one 예시
docker run -d -p 4317:4317 -p 16686:16686 jaegertracing/all-in-one:1.57
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
cargo run --release
# → http://localhost:16686 에서 service.name=hdmeal-backend 으로 조회
```

### 전송 항목

- **Span exporter**: OTLP/gRPC (tonic), 5초 타임아웃
- **Sampler**: `AlwaysOn`
- **Resource attributes**: `service.name`, `service.version`
- **Propagator**: W3C `TraceContext` (`traceparent` / `tracestate`) + `BaggagePropagator`
- **Span source**: `tracing` 매크로 (`info!`, `warn!`, `error!`) 와 `warp::trace::request()` 및 custom Warp 필터
  가 자동 계측. `#[instrument]` 매크로를 함수에 붙여 span 을 더 세분화 가능.

### 의도적 제외

- OTel **metrics** SDK — 라벨 카디널리티가 낮고 카운터는 단순 hashmap으로 충분.
  현재 `/metrics` 엔드포인트는 자체 `Metrics` 구조체로 노출. 히스토그램이 필요해지면 `prometheus` 크레이트로 교체.

## 배포(CI/CD)

GitHub Actions로 amd64/arm64 Docker 이미지를 빌드/푸시합니다.

### Container Registry (GHCR)

이미지는 `ghcr.io/hyunbridge/hdmeal-backend:<tag>` 형태로 푸시됩니다.

- `main` 브랜치 push 시 `main`, `sha`, `latest` 태그를 발행합니다.
- `v*.*.*` 태그 push 시 semver 태그와 `latest` 태그를 발행합니다.

### 런타임 환경변수

런타임 환경변수는 서버 측 `.env` 파일에서 주입합니다. 주요 항목:

- `MONGODB_URI`, `MONGODB_DATABASE`
- `NEIS_OPENAPI_TOKEN`, `ATPT_OFCDC_SC_CODE`, `SD_SCHUL_CODE`
- `NUM_OF_GRADES`, `NUM_OF_CLASSES`
- `HDMeal_AuthTokens`, `HDMeal_JWTSecret`
- `HDMeal_SeoulData_Token`, `HDMeal_KMA_ApiKey`, `HDMeal_KMA_NX`, `HDMeal_KMA_NY`
- `HDMeal_BaseURL`
- `HDMeal_AllowedOrigins`(선택)
- `HDMeal_MaxDaysRange`(선택), `HDMeal_AppVersion`(선택), `HDMeal_AppBuild`(선택)

## License

MIT
