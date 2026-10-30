# Neolink Docker image build scripts
# Copyright (c) 2020 George Hilliard,
#                    Andrew King,
#                    Miroslav Šedivý
# SPDX-License-Identifier: AGPL-3.0-only

# ---------------------------------------------------------------------------
# 2026-09-09 base moved bookworm -> trixie.
#
# An earlier fork revision shipped a LOCALLY PATCHED
# libgstrtspserver-1.0-0 1.22.0-2+deassert.1
# (three g_assert sites in gst_rtsp_media_get_rates routed onto the function's
# graceful result=FALSE path) because a dead camera's complete-but-frameless
# sender stream SIGABRTed the whole process on every client PLAY.
#
# Upstream fixed the same thing: gstreamer MR !7731 (merged 2024-10-24) deletes
# both `g_assert (FALSE)` sites in gst_rtsp_media_get_rates. Debian trixie ships
# gst-rtsp-server 1.26.2-1, which contains that fix -- VERIFIED by reading
# trixie's own `apt-get source` of rtsp-media.c (both g_assert(FALSE) gone, the
# `result = FALSE; break;` paths remain) and by objdump on the shipped .so:
# bookworm 1.22.0-2 = 3 g_assertion_message_expr calls in that symbol,
# trixie 1.26.2-1 = 1 (the surviving `g_assert (priv->streams->len > 0)`
# guard, which is NOT the dead-camera vector -- neolink's factories always
# mount at least pay0).
#
# So the whole gstrtsp-patched build stage + the deb-over-distro install are
# GONE. docker/gst-rtsp-media-deassert.patch is retained for the rollback path
# only (rollback = rebuild the previous revision of this Dockerfile,
# which carries the patched deb).
# Every Rust-side fix (DESCRIBE liveness gate, egress watchdog, placeholder
# handling) is unchanged -- this commit touches only the Dockerfile.
#
# Base images pinned by tag + digest.
# ---------------------------------------------------------------------------

FROM docker.io/rust:slim-trixie@sha256:bce1476d4be4d78b83705bc5f428b86d640eeeea33e9dadafbc037b5703a53bf AS build
ARG TARGETPLATFORM

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /usr/local/src/neolink
COPY . /usr/local/src/neolink

# Build the main program or copy from artifact
#
# We prefer copying from artifact to reduce
# build time on the github runners
#
# hadolint ignore=DL3008
RUN  echo "TARGETPLATFORM: ${TARGETPLATFORM}"; \
  if [ -f "${TARGETPLATFORM}/neolink" ]; then \
    echo "Restoring from artifact"; \
    mkdir -p /usr/local/src/neolink/target/release/; \
    cp "${TARGETPLATFORM}/neolink" "/usr/local/src/neolink/target/release/neolink"; \
  else \
    echo "Building from scratch"; \
    apt-get update && \
        apt-get upgrade -y && \
        apt-get install -y --no-install-recommends \
          build-essential \
          openssl \
          libssl-dev \
          ca-certificates \
          libgstrtspserver-1.0-dev \
          libgstreamer1.0-dev \
          libgtk2.0-dev \
          protobuf-compiler \
          libglib2.0-dev && \
        apt-get clean -y && rm -rf /var/lib/apt/lists/* ; \
    pkg-config --modversion gstreamer-rtsp-server-1.0 ; \
    cargo build --release; \
  fi

# Create the release container. Match the base OS used to build
FROM debian:trixie-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132
ARG TARGETPLATFORM
ARG REPO
ARG VERSION
ARG OWNER

LABEL description="An image for the neolink program which is a reolink camera to rtsp translator"
LABEL repository="$REPO"
LABEL version="$VERSION"
LABEL maintainer="$OWNER"

# bind9-dnsutils replaces bookworm's transitional `dnsutils` (dropped in trixie).
# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        openssl \
        bind9-dnsutils \
        iputils-ping \
        ca-certificates \
        libgstrtspserver-1.0-0 \
        libgstreamer1.0-0 \
        gstreamer1.0-tools \
        gstreamer1.0-x \
        gstreamer1.0-plugins-base \
        gstreamer1.0-plugins-good \
        gstreamer1.0-plugins-bad \
        gstreamer1.0-libav && \
    apt-get clean -y && rm -rf /var/lib/apt/lists/*

# Guard: the runtime MUST be a stock gst-rtsp-server >= 1.24.9 (i.e. one that
# carries MR !7731). Fail the build loudly if a future base regresses either
# the version or the local-suffix cleanliness.
RUN set -eu; \
    v="$(dpkg-query -W -f='${Version}' libgstrtspserver-1.0-0)"; \
    echo "libgstrtspserver-1.0-0 = $v"; \
    case "$v" in *deassert*) echo "ERROR: patched deb present, expected stock" >&2; exit 1;; esac; \
    dpkg --compare-versions "$v" ge 1.24.9 || { echo "ERROR: $v < 1.24.9 (missing MR !7731)" >&2; exit 1; }

COPY --from=build \
  /usr/local/src/neolink/target/release/neolink \
  /usr/local/bin/neolink
COPY docker/entrypoint.sh /entrypoint.sh

RUN gst-inspect-1.0 > /dev/null; \
    chmod +x "/usr/local/bin/neolink" && \
    "/usr/local/bin/neolink" --version && \
    mkdir -m 0700 /root/.config/

ENV NEO_LINK_MODE="rtsp" NEO_LINK_PORT=8554

# Report unhealthy if the bridge is up but no longer serving frames (a frozen
# pipeline keeps the process "running" and the RTSP port open, so only pulling
# a frame catches it). Many-camera setups may need a longer --timeout, or can
# check a single camera.
HEALTHCHECK --interval=60s --timeout=30s --start-period=120s --retries=3 \
  CMD /usr/local/bin/neolink healthcheck --config /etc/neolink.toml

CMD /usr/local/bin/neolink "${NEO_LINK_MODE}" --config /etc/neolink.toml
ENTRYPOINT ["/entrypoint.sh"]
EXPOSE ${NEO_LINK_PORT}
