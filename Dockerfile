# Neolink Docker image build scripts
# Copyright (c) 2020 George Hilliard,
#                    Andrew King,
#                    Miroslav Šedivý
# SPDX-License-Identifier: AGPL-3.0-only

FROM docker.io/rust:slim-bookworm AS build
ARG TARGETPLATFORM

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /usr/local/src/neolink
COPY . /usr/local/src/neolink

# Build the main program or copy from artifact
#
# We prefer copying from artifact to reduce
# build time on the github runners
#
# Because of this though, during normal
# github runner ops we are not testing the
# docker to see if it will build from scratch
# so if it is failing please make a PR
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
    cargo build --release; \
  fi

# ---------------------------------------------------------------------------
# local patch: gst-rtsp-server 1.22 (de-assert gst_rtsp_media_get_rates).
#
# A dead camera can leave a stream in "complete sender but no data ever
# flowed" state; every client PLAY then hits g_assert(FALSE) in
# rtsp-media.c:2766 -> SIGABRT of the WHOLE process (~130 crashes/hour on
# 2026-08-14, all cameras down each time). The C library is Debian's, not
# neolink's Rust, so we rebuild the bookworm package with a 3-site patch
# routing the asserts onto the function's existing graceful result=FALSE
# path (caller logs "failed to obtain consistent rate", errors that one
# client's request). See docker/gst-rtsp-media-deassert.patch.
#
# The built .deb (version-suffixed +deassert.1) is installed over the distro
# lib in the runtime stage below. The Rust build stage intentionally keeps
# the UNPATCHED -dev package: the patch changes no headers/ABI, only .so
# internals.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS gstrtsp-patched
ENV DEBIAN_FRONTEND=noninteractive
# hadolint ignore=DL3008
RUN sed -i 's/^Types: deb$/Types: deb deb-src/' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential devscripts dpkg-dev quilt fakeroot && \
    apt-get build-dep -y gst-rtsp-server1.0
WORKDIR /build
COPY docker/gst-rtsp-media-deassert.patch /build/
RUN apt-get source gst-rtsp-server1.0 && \
    cd gst-rtsp-server1.0-*/ && \
    patch -p1 --fuzz=0 < /build/gst-rtsp-media-deassert.patch && \
    # patch applied cleanly or the build dies here; belt-and-braces: the
    # three de-assert marker comments must be present in the patched source
    test "$(grep -c 'de-assert' gst/rtsp-server/rtsp-media.c)" -ge 3 && \
    DEBEMAIL=deassert@local DEBFULLNAME="neolink deassert" \
      dch --local "+deassert." "de-assert gst_rtsp_media_get_rates: dead-camera PLAY must not SIGABRT the process" && \
    DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage -b -uc -us && \
    ls -l /build/libgstrtspserver-1.0-0_*.deb

# Create the release container. Match the base OS used to build
FROM debian:bookworm-slim
ARG TARGETPLATFORM
ARG REPO
ARG VERSION
ARG OWNER

LABEL description="An image for the neolink program which is a reolink camera to rtsp translator"
LABEL repository="$REPO"
LABEL version="$VERSION"
LABEL maintainer="$OWNER"

# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        openssl \
        dnsutils \
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

# install the de-asserted libgstrtspserver over the distro one
# (same upstream version, +deassert.1 local suffix -> dpkg treats it as an
# upgrade; hold so a hypothetical apt upgrade can't silently revert it).
COPY --from=gstrtsp-patched /build/libgstrtspserver-1.0-0_*.deb /tmp/
RUN dpkg -i /tmp/libgstrtspserver-1.0-0_*.deb && \
    rm -f /tmp/libgstrtspserver-1.0-0_*.deb && \
    apt-mark hold libgstrtspserver-1.0-0 && \
    dpkg -s libgstrtspserver-1.0-0 | grep -i '^Version:.*deassert'

COPY --from=build \
  /usr/local/src/neolink/target/release/neolink \
  /usr/local/bin/neolink
COPY docker/entrypoint.sh /entrypoint.sh

RUN gst-inspect-1.0; \
    chmod +x "/usr/local/bin/neolink" && \
    "/usr/local/bin/neolink" --version && \
    mkdir -m 0700 /root/.config/

ENV NEO_LINK_MODE="rtsp" NEO_LINK_PORT=8554

CMD /usr/local/bin/neolink "${NEO_LINK_MODE}" --config /etc/neolink.toml
ENTRYPOINT ["/entrypoint.sh"]
EXPOSE ${NEO_LINK_PORT}

