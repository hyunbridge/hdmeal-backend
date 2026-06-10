# syntax=docker/dockerfile:1.7
#
# ---- builder ----
# rust:1.88-slim-trixie (glibc) + Release LTO=thin.
# 결과 바이너리는 glibc 동적 링크. 최종 이미지는 distroless/cc-debian13 (glibc 호환).
#
# ---- multi-arch + CPU-specific RUSTFLAGS ----
# BuildKit 이 `--platform=linux/amd64,linux/arm64` 로 호출 시 TARGETARCH 를
# 자동 주입한다. arch 별로 다른 RUSTFLAGS 를 cargo build 에 inline 전달.
#
# - amd64 (x86_64): x86-64-v3 + SHA-NI
#   * x86-64-v3 = Haswell+ (2013 Intel, 2017 AMD Zen). AVX2 + FMA + BMI1/2 + LZCNT + MOVBE + F16C.
#     SIMD vectorize 가능한 모든 hot-path (serde, regex, json, bson) 가속.
#   * +sha = Intel SHA-NI / AMD SHA Extensions (Zen 1 부터 모든 서버 CPU).
#     `sha2` crate, `jsonwebtoken` HS256 HMAC 가속.
#
# - arm64 (aarch64): neoverse-n1 + sha2 + aes
#   * neoverse-n1 = AWS Graviton 2/3 (Cortex-A76 microarch). 모든 ARMv8.2+ 호환:
#     Apple Silicon M1~M4, Graviton 2/3/4, Ampere Altra 모두 실행 가능.
#   * +sha2 = ARMv8 SHA-256 instruction (모든 Apple Silicon, Graviton, Ampere 지원).
#   * +aes = ARMv8 AES (mongodb TLS 가속).
FROM rust:1.88-slim-trixie AS builder

ARG TARGETARCH

WORKDIR /app

# 시스템 의존성
# - ca-certificates: build stage 에서 HTTPS registry/API 접근
# - pkg-config: native dependency probing 이 필요한 transitive crate 대비
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Build cache 효율: 의존성만 먼저 빌드
COPY Cargo.toml Cargo.lock* ./
RUN case "$TARGETARCH" in \
        amd64) RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+sha" ;; \
        arm64) RUSTFLAGS="-C target-cpu=neoverse-n1 -C target-feature=+sha2,+aes" ;; \
        *)     RUSTFLAGS="" ;; \
    esac && \
    echo "Building deps for $TARGETARCH with RUSTFLAGS=\"$RUSTFLAGS\"" && \
    mkdir -p src && echo "fn main(){}" > src/main.rs && echo "" > src/lib.rs \
    && CARGO_TARGET_DIR=/app/target RUSTFLAGS="$RUSTFLAGS" cargo build --release --locked --bin hdmeal-backend \
    && rm -rf \
        /app/target/release/.fingerprint/hdmeal-backend-* \
        /app/target/release/deps/hdmeal_backend* \
        /app/target/release/deps/libhdmeal_backend* \
        /app/target/release/hdmeal-backend*

# 실제 소스 빌드
COPY src ./src
COPY data ./data

ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

RUN case "$TARGETARCH" in \
        amd64) RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+sha" ;; \
        arm64) RUSTFLAGS="-C target-cpu=neoverse-n1 -C target-feature=+sha2,+aes" ;; \
        *)     RUSTFLAGS="" ;; \
    esac && \
    echo "Building for $TARGETARCH with RUSTFLAGS=\"$RUSTFLAGS\"" && \
    RUSTFLAGS="$RUSTFLAGS" cargo build --release --locked --bin hdmeal-backend \
    && ls -la /app/target/release/hdmeal-backend

# ---- runtime ----
# distroless/cc-debian13: glibc + libgcc + ca-certificates + /etc/passwd(/etc/group)
# 패키지 매니저 / shell 없음 → 최소 attack surface. tini 불필요 (k8s/container runtime init 사용).
# read-only rootfs 호환: 런타임 파일 쓰기 없음.
#   k8s:       securityContext.readOnlyRootFilesystem: true
#   docker:    docker run --read-only ...
#   compose:   read_only: true
FROM gcr.io/distroless/cc-debian13:nonroot AS runtime

WORKDIR /app

# 빌드 산출물
COPY --from=builder /app/target/release/hdmeal-backend /usr/local/bin/hdmeal-backend

ENV PORT=8000 \
	RUST_LOG=info \
	OTEL_SERVICE_NAME=hdmeal-backend

# nonroot (uid 65532) 가 distroless 에 기본 존재
USER nonroot
EXPOSE 8000

# distroless 에는 shell/curl 이 없으므로, 동일 바이너리의 `--healthcheck` 모드로
# localhost `/healthz` 를 확인한다.
HEALTHCHECK --interval=10s --timeout=2s --start-period=90s --retries=3 \
    CMD ["/usr/local/bin/hdmeal-backend", "--healthcheck"]

ENTRYPOINT ["/usr/local/bin/hdmeal-backend"]
