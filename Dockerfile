# syntax=docker/dockerfile:1.7
#
# ---- builder ----
# rust:1.88-slim-trixie (glibc) + mold 링커 + Release LTO=fat.
# 결과 바이너리는 glibc 동적 링크. 최종 이미지는 distroless/cc-debian13 (glibc 호환).
FROM rust:1.88-slim-trixie AS builder

WORKDIR /app

# 시스템 의존성
# - pkg-config, ca-certificates: reqwest/rustls, mongo driver 빌드용
# - mold: lld/ld 대비 수배 빠른 linker
# - clang/lld: aws_lc_rs 가 의존할 가능성 대비 (현재는 ring 사용으로 불필요하지만 안전망)
#   => jsonwebtoken 을 ring 으로 바꿔서 aws_lc_rs 제거됨 — clang/lld 제거 가능
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config ca-certificates mold \
    && rm -rf /var/lib/apt/lists/*

# Build cache 효율: 의존성만 먼저 빌드
COPY Cargo.toml Cargo.lock* ./
RUN mkdir -p src && echo "fn main(){}" > src/main.rs && echo "" > src/lib.rs \
    && CARGO_TARGET_DIR=/app/target cargo build --release --bin hdmeal-backend \
    && rm -rf /app/target/release/deps/hdmeal_backend* /app/target/release/hdmeal-backend*

# 실제 소스 빌드
COPY src ./src
COPY data ./data

# 빌드 인자 (multi-arch 대응):
#   TARGETPLATFORM: linux/amd64 | linux/arm64
ARG TARGETARCH

# 컴파일러/링커 환경
# - CARGO_BUILD_RUSTFLAGS: target-cpu 만 arch 별로 다르게 (x86-64-v3 / neoverse-n1)
# - 나머지(lto, panic, strip)는 Cargo.toml profile.release.* 에서 처리
# - mold: 같은 디렉터리에 cc/ar 프록시 + CARGO_BIN_EXE_mold 호출. PATH 에 mold 만 두면
#   cargo 가 자동으로 mold 호출. 동적 링커도 mold 가 처리.
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

RUN if [ "$TARGETARCH" = "amd64" ]; then \
        EXTRA_FLAGS="-C target-cpu=x86-64-v3"; \
    else \
        EXTRA_FLAGS="-C target-cpu=neoverse-n1"; \
    fi \
    && CARGO_BUILD_RUSTFLAGS="$EXTRA_FLAGS" \
       cargo build --release --bin hdmeal-backend \
    && strip /app/target/release/hdmeal-backend \
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
