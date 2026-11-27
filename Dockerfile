# syntax=docker/dockerfile:1
#
# Runtime-only image. The BSV Broadcaster binary is built in CI on a native runner per
# architecture and copied in here, so this Dockerfile contains no Go toolchain
# and no cross-compilation. To build locally, run `make docker-build` (which
# produces the dist/linux-<arch>/bsv-broadcaster layout this Dockerfile expects).
#
# The binary is built with CGO_ENABLED=1 — it links the BSV BDK C++ consensus
# library used by the transaction validator (see the validator package), so it
# is dynamically linked against glibc + libstdc++ and will NOT run on a musl
# base such as alpine. The distroless/cc base below provides glibc, libstdc++,
# libgcc and ca-certificates.
#
# ca-certificates is required: BSV Broadcaster is an outbound HTTPS client (Teranode,
# merkle service, datahub, webhook delivery) and TLS handshakes fail without
# the system CA bundle. distroless/cc ships it, so no extra install step.

# Pinned by digest (OpenSSF Scorecard Pinned-Dependencies). The digest is the
# multi-arch image index for :latest, not a single-platform manifest, so the
# matrix amd64/arm64 build in .github/workflows/build.yml still resolves the
# right platform. dependabot's docker ecosystem (.github/dependabot.yml) bumps
# the digest when upstream republishes :latest.
FROM gcr.io/distroless/cc-debian12:latest@sha256:e5d81ddde149641e2a9ba55be4545bc125c67de07508b03ba4c22e6eb0ded5aa

ARG TARGETOS
ARG TARGETARCH
COPY dist/${TARGETOS}-${TARGETARCH}/bsv-broadcaster /usr/local/bin/bsv-broadcaster

ENTRYPOINT ["/usr/local/bin/bsv-broadcaster"]
