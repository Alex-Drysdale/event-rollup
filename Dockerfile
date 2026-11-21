# Native Rust pipeline.
FROM rust:1.100-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# The scorers are Intel Xeon (Cascade Lake or newer, AVX-512).
ARG TARGET_CPU=cascadelake
RUN RUSTFLAGS="-C target-cpu=${TARGET_CPU} -C target-feature=+crt-static" \
    cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/sessions /sessions

# A single static binary: nothing else in the image, so the container starts fast.
FROM scratch
COPY --from=build /sessions /sessions
USER 1000:1000
CMD ["/sessions"]
