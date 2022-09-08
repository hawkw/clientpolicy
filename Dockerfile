
ARG RUST_IMAGE=docker.io/library/rust:1.63.0
ARG RUNTIME_IMAGE=gcr.io/distroless/cc

# Builds the controller binary.
FROM $RUST_IMAGE as build
ARG TARGETARCH
ARG BUILD_TYPE="release"
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src src
RUN --mount=type=cache,target=target \
    --mount=type=cache,from=rust:1.63.0,source=/usr/local/cargo,target=/usr/local/cargo \
    cargo fetch
RUN --mount=type=cache,target=target \
    --mount=type=cache,from=rust:1.63.0,source=/usr/local/cargo,target=/usr/local/cargo \
    if [ "$BUILD_TYPE" = debug ]; then \
    cargo build --frozen --target=x86_64-unknown-linux-gnu --package=client-policy-controller && \
    mv target/x86_64-unknown-linux-gnu/debug/client-policy-controller /tmp/ ; \
    else \
    cargo build --frozen --target=x86_64-unknown-linux-gnu --release --package=client-policy-controller && \
    mv target/x86_64-unknown-linux-gnu/release/client-policy-controller /tmp/ ; \
    fi

# Creates a minimal runtime image with the controller binary.
FROM $RUNTIME_IMAGE
COPY --from=build /tmp/client-policy-controller /bin/
ENTRYPOINT ["/bin/client-policy-controller"]