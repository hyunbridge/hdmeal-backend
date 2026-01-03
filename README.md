# HDMeal Backend

레거시 Flask 서버([hdmeal-chatbot](https://github.com/hyunbridge/hdmeal-chatbot), [hdmeal-json](https://github.com/hyunbridge/hdmeal-json))를 대체하기 위해 작성된 흥덕고 급식봇/모바일 앱용 통합 FastAPI 백엔드입니다.

## 기능

- 모바일 앱용 통합 데이터 API(급식/학사일정/시간표): `/api/app/*`
- 카카오 i 오픈빌더 Skill 엔드포인트: `/skill/`
- 사용자 설정(학년/반, 알레르기 표기 등): `/user/settings/`
- 캐시 상태 점검: `/cache/healthcheck/`
- 주기적 데이터 동기화(NEIS + 보조 API) 및 MongoDB 캐싱

## 기술 스택

- Python 3.10+
- FastAPI + Uvicorn
- MongoDB (pymongo async)
- Pydantic v2 + pydantic-settings
- httpx(외부 API 호출), Authlib(JWT)

## 프로젝트 구조

- `app/main.py`: 앱 초기화, CORS, 라우터 등록, 주기 작업(lifespan)
- `app/config.py`: 환경변수(Settings) 정의 및 파싱
- `app/routers/`: HTTP API 라우터
  - `app_api.py`: 모바일 앱 API(`/api/app/*`)
  - `chatbot.py`: 카카오 스킬/사용자 설정/캐시 점검
- `app/services/`: 도메인 서비스 계층
  - `DataService`: MongoDB 컬렉션/인덱스/CRUD 캡슐화
  - `IngestionService`: NEIS/날씨/수온 데이터 동기화
  - `ChatbotService`: (레거시와 호환되는) 응답 생성 로직
- `app/ingestion/`: 외부 데이터 소스 커넥터
  - `neis.py`: NEIS(급식/학사/시간표) 수집
  - `auxiliary.py`: KMA(날씨), 서울 열린데이터(한강 수온)

## 데이터 흐름(캐시)

- 요청 처리 시 `IngestionService.sync_range()`로 조회 구간을 먼저 동기화한 뒤, MongoDB 캐시에서 데이터를 읽어 응답합니다.
- 앱 시작 시 10일 전~10일 후 구간을 워밍업하고, 이후 3시간 간격으로 주기 동기화를 수행합니다.
- MongoDB 컬렉션: `meals`, `schedules`, `timetables`, `weather`, `water_temperatures`, `users`

## 인증/권한

- `X-HDMeal-Token`: 카카오 스킬 호출 인증(서버가 보유한 `HDMeal_AuthTokens`와 비교)
- 사용자 설정 API: `X-HDMeal-Token`에 JWT를 사용하며 scope 기반으로 권한을 검사합니다.

## 주요 Endpoint

- `GET /healthz`: 헬스 체크
- `GET /api/app/days?from=YYYY-MM-DD&to=YYYY-MM-DD`: 기간 통합 데이터 조회
- `GET /api/app/days/{YYYY-MM-DD}`: 단일 일자 통합 데이터 조회
- `GET /api/app/meta`: 앱 버전/빌드 메타 정보
- `POST /skill/`: 카카오 i 오픈빌더 Skill 요청 처리
- `GET|PATCH|DELETE /user/settings/`: 사용자 설정 조회/수정/삭제
- `GET /cache/healthcheck/`: 캐시 TTL 기반 상태 반환

## 로컬 개발

```bash
pip install -e .
cp .env.example .env
uvicorn app.main:app --reload
```

## 환경 변수

런타임 필수 값은 `hdmeal-backend/.env.example`에 정의되어 있습니다.

### 필수(런타임)

- `MONGODB_URI`, `MONGODB_DATABASE`: MongoDB 연결 정보
- `NEIS_OPENAPI_TOKEN`, `ATPT_OFCDC_SC_CODE`, `SD_SCHUL_CODE`: NEIS OpenAPI 인증/학교 식별자
- `NUM_OF_GRADES`, `NUM_OF_CLASSES`: 시간표/사용자 설정 범위(학년/반)
- `HDMeal_AuthTokens`: 카카오 스킬 인증 토큰(JSON 배열 또는 단일 문자열)
- `HDMeal_JWTSecret`: 사용자 설정 JWT 서명 키
- `HDMeal_SeoulData_Token`: 서울 열린데이터(한강 수온) API 키
- `HDMeal_KMAZone`: KMA 동네예보 zone 코드
- `HDMeal_BaseURL`: 사용자 설정 웹 베이스 URL(카드 링크/Allowed Origins 계산에 사용)

### 선택(런타임)

- `HDMeal_AllowedOrigins`: CORS 허용 Origin(쉼표 구분 또는 JSON 배열). 미설정 시 개발용 origin이 자동 포함됩니다.
- `HDMeal_reCAPTCHA_Token`: reCAPTCHA 검증용 secret(미설정 시 검증 실패로 처리)
- `HDMeal_MaxDaysRange`: `/api/app/days` 최대 조회 범위(기본 31일)
- `HDMeal_AppVersion`, `HDMeal_AppBuild`: `/api/app/meta` 응답 값

## 배포(CI/CD)

GitHub Actions는 Docker 이미지 빌드/푸시 후 SSH로 서버에 접속해 컨테이너를 재기동합니다.

### GitHub Secrets

배포 시 환경변수는 **서버 `.env`** 또는 **GitHub Secrets** 중 하나(또는 둘 다)에서 주입할 수 있습니다.

- 기본값: `both` (서버 `.env` + GitHub Secrets를 함께 사용, Secrets가 있으면 override)
- 모드 선택: GitHub Repository Variable `DEPLOY_ENV_SOURCE`에 `server` | `secrets` | `both` 설정

### Container Registry (GHCR)

이미지는 GitHub Container Registry(GHCR, `ghcr.io`)에 푸시합니다.

- 별도 Registry 자격증명 Secret 없이 `GITHUB_TOKEN`으로 푸시합니다(워크플로우에 `packages: write` 권한 포함).
- 서버에서는 `docker pull ghcr.io/<owner>/<repo>/hdmeal-backend:latest` 형태로 pull 합니다.

**SSH 배포**
- `SSH_HOST`, `SSH_USERNAME`, `SSH_PRIVATE_KEY`, `SSH_PORT`
- `API_PORT`(선택), `DEPLOY_PATH`(선택: 서버에 `.env`를 둘 경우)

**애플리케이션 런타임(서버 `.env` 또는 Secrets로 주입)**
- `MONGODB_URI`, `MONGODB_DATABASE` (DB 연결정보도 서버 `.env`로 주입 가능)
- `NEIS_OPENAPI_TOKEN`, `ATPT_OFCDC_SC_CODE`, `SD_SCHUL_CODE`
- `NUM_OF_GRADES`, `NUM_OF_CLASSES`
- `HDMeal_AuthTokens`, `HDMeal_JWTSecret`
- `HDMeal_SeoulData_Token`, `HDMeal_KMAZone`
- `HDMeal_BaseURL`
- `HDMeal_AllowedOrigins`(선택), `HDMeal_reCAPTCHA_Token`(선택)
- `HDMeal_MaxDaysRange`(선택), `HDMeal_AppVersion`(선택), `HDMeal_AppBuild`(선택)

## 라이선스

MIT
