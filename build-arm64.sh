#!/bin/sh

# To be used inside a rust docker container for reproducible builds

set -e

rustup target add aarch64-unknown-linux-musl

export DEBIAN_FRONTEND=noninteractive
apt-get update && apt-get install -y --no-install-recommends wget make perl && rm -rf /var/lib/apt/lists/*

wget -O - https://musl.cc/aarch64-linux-musl-native.tgz | tar -xz

export TARGET_CC=/aarch64-linux-musl-native/bin/aarch64-linux-musl-gcc
export CC_aarch64_unknown_linux_musl=$TARGET_CC
export RUSTC_LINKER=$TARGET_CC

cd /code

cargo build --release --target aarch64-unknown-linux-musl
