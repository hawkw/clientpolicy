
ARG RUST_IMAGE=docker.io/library/rust:1.63.0
ARG RUNTIME_IMAGE=gcr.io/distroless/cc

# Builds the controller binary.
FROM $RUST_IMAGE as build
ARG TARGETARCH
ARG BUILD_TYPE="release"
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY k8s k8s
COPY src src
RUN --mount=type=cache,target=target \
    --mount=type=cache,from=rust:1.63.0,source=/usr/local/cargo,target=/usr/local/cargo \
    cargo fetch
RUN --mount=type=cache,target=target \
    --mount=type=cache,from=rust:1.63.0,source=/usr/local/cargo,target=/usr/local/cargo \
    if [ "$BUILD_TYPE" = debug ]; then \
    cargo build --frozen --target=x86_64-unknown-linux-gnu --package=linkerd-client-policy && \
    mv target/x86_64-unknown-linux-gnu/debug/linkerd-client-policy /tmp/ ; \
    else \
    cargo build --frozen --target=x86_64-unknown-linux-gnu --release --package=linkerd-client-policy && \
    mv target/x86_64-unknown-linux-gnu/release/linkerd-client-policy /tmp/ ; \
    fi

# Creates a minimal runtime image with the controller binary.
FROM $RUNTIME_IMAGE
COPY --from=build /tmp/linkerd-client-policy /bin/
ENTRYPOINT ["/bin/linkerd-client-policy"]