# syntax=docker/dockerfile:1.7
#
# ---- builder ----
# rust:1.88-slim-trixie (glibc) + Release LTO=fat.
# 결과 바이너리는 glibc 동적 링크. 최종 이미지는 distroless/cc-debian13 (glibc 호환).
FROM rust:1.88-slim-trixie AS builder

WORKDIR /app

# 시스템 의존성
# - ca-certificates: build stage 에서 HTTPS registry/API 접근
# - pkg-config: native dependency probing 이 필요한 transitive crate 대비
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Build cache 효율: 의존성만 먼저 빌드
COPY Cargo.toml Cargo.lock* ./
RUN mkdir -p src && echo "fn main(){}" > src/main.rs && echo "" > src/lib.rs \
    && CARGO_TARGET_DIR=/app/target cargo build --release --bin hdmeal-backend \
    && rm -rf /app/target/release/deps/hdmeal_backend* /app/target/release/hdmeal-backend*

# 실제 소스 빌드
COPY src ./src
COPY data ./data

ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

RUN cargo build --release --bin hdmeal-backend \
    && ls -la /app/target/release/hdmeal-backend

# ---- runtime ----
# distroless/cc-debian13: glibc + libgcc + ca-certificates + /etc/passwd(/etc/group)
# 패키지 매니저 / shell 없음 → 최소 attack surface. tini 불필요 (k8s/container runtime init 사용).
FROM gcr.io/distroless/cc-debian13:nonroot AS runtime

WORKDIR /app

# 빌드 산출물 + 정적 데이터
COPY --from=builder /app/target/release/hdmeal-backend /usr/local/bin/hdmeal-backend
COPY --from=builder /app/data /app/data

ENV DATA_DIR=/app/data \
    PORT=8000 \
    RUST_LOG=info \
    OTEL_SERVICE_NAME=hdmeal-backend

# nonroot (uid 65532) 가 distroless 에 기본 존재
USER nonroot
EXPOSE 8000

ENTRYPOINT ["/usr/local/bin/hdmeal-backend"]
