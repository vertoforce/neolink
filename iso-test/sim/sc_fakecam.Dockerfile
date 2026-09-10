# Packages the fake Baichuan camera (iso-test/sim/fakecam) as a container so it
# can sit on an isolated docker network next to the neolink container under
# test. The binary is built outside this Dockerfile -- see sc_lib.sh's
# sc_build_fakecam_image, which compiles it in a Rust-toolchain image and then
# stages binary + data + ctl.py into a tiny build context.
#
#   docker build -f sc_fakecam.Dockerfile -t neolink-sc-fakecam:latest <staging-dir>
#
# Runtime: BC protocol on :9000, fault-injection control on :9010, both bound
# to 0.0.0.0 so a sibling container can reach them.
FROM debian:trixie-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-minimal ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY fakecam /usr/local/bin/fakecam
COPY ctl.py  /usr/local/bin/ctl.py
COPY data/   /opt/fakecam/data/

RUN chmod +x /usr/local/bin/fakecam /usr/local/bin/ctl.py

EXPOSE 9000 9010
ENTRYPOINT ["/usr/local/bin/fakecam"]
CMD ["--bind", "0.0.0.0:9000", "--control", "0.0.0.0:9010"]
