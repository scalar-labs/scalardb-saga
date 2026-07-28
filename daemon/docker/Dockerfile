# syntax=docker/dockerfile:1

# Build context is daemon/build/docker, assembled by the `dockerContext` Gradle task — not this
# directory and not the repository root. Build it with `./gradlew :daemon:dockerBuild` locally, or
# via docker/build-push-action in the release workflow. The distribution is built by Gradle on the
# host and only packaged here: building it inside a stage instead would re-resolve every dependency
# on each image build and lose the build cache CI already has.

# Tag and digest together: the tag documents what this is, the digest is what actually gets pulled, so
# rebuilding a commit yields the same base layer and a silently-retagged upstream cannot change what
# ships. Dependabot updates both (see .github/dependabot.yml).
#
# A JRE, not a JDK: the daemon runs a start script and needs no compiler, so shipping javac would add
# tooling that is useful to an attacker inside the container and to nobody else.
FROM eclipse-temurin:21-jre-noble@sha256:373787d1d45a87f084fda43e7de0e9acf5eedee049446efac738f13587ec4c64

# Two things in one layer, since each `RUN` is a layer and neither is worth its own:
#
# 1. Upgrade the OS packages. The base image is rebuilt on its own cadence, so at any moment it can
#    carry packages whose CVEs are already fixed upstream; upgrading at build time closes that window
#    without waiting for a base image respin. hadolint flags apt-get upgrade (DL3005) as making builds
#    non-deterministic — which is the intent, and the digest pin above fixes the starting point.
# 2. Create a fixed numeric uid/gid, matching the convention the other Scalar server images use, so a
#    Kubernetes runAsNonRoot admission check passes without a passwd lookup and any mounted volume's
#    ownership stays predictable across upgrades.
#
# hadolint ignore=DL3005
RUN apt-get update \
 && apt-get upgrade -y --no-install-recommends \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 201 scalar \
 && useradd --system --uid 201 --gid 201 --no-create-home scalar

WORKDIR /scalardb-saga

# The `installDist` tree, not the distribution tar: the tar's internal root directory embeds the
# version (scalardb-saga-daemon-1.2.3/), which would make every path in this file change each
# release. Copied before the configuration so that editing the config does not invalidate the ~150 MB
# jar layer.
COPY --chown=201:201 dist/ ./

# A template, not a working configuration: the image cannot serve anything until an operator supplies
# real ScalarDB connection settings and at least one saga definition. Mount over /scalardb-saga/conf
# (e.g. a ConfigMap plus a definitions volume) or derive an image that bakes them in.
COPY --chown=201:201 conf/ ./conf/

# The product license plus generated attributions for every third-party jar vendored into the layer
# above. /licenses is where the Red Hat/OpenShift convention looks, and a predictable location
# regardless of platform.
COPY LICENSE THIRD-PARTY-NOTICES.txt /licenses/

USER 201:201

# The SagaServerConfig defaults: 8080 REST, 50051 gRPC. Both transports are on by default and either
# can be disabled in the configuration, in which case its port simply goes unbound.
EXPOSE 8080 50051

# No entrypoint wrapper script. Container-aware JVM flags (MaxRAMPercentage, ExitOnOutOfMemoryError)
# are baked into the start script by Gradle's applicationDefaultJvmArgs, and JAVA_OPTS still
# overrides them at run time — so the shell hop the other Scalar images use to assemble JAVA_OPTS
# buys nothing here. The script ends in `exec java`, so the JVM becomes PID 1 and receives SIGTERM
# directly; SagaServer's shutdown hook then drains in-flight sagas rather than dropping them.
#
# No grpc_health_probe binary either: Kubernetes has had native gRPC probes since 1.24 (GA in 1.27),
# and the daemon serves both `GET /health` and the standard grpc.health.v1.Health service — the
# latter deliberately left unauthenticated so a gRPC-only deployment stays probeable.
ENTRYPOINT ["/scalardb-saga/bin/scalardb-saga-daemon"]
CMD ["/scalardb-saga/conf/server.properties"]

# Declared last: these change on every release, and anything below a changed instruction is rebuilt.
# org.opencontainers.image.created is deliberately absent — a build timestamp would give the image a
# different digest on every rebuild of identical inputs. Buildx records the build time in provenance
# attestation instead.
#
# image.documentation points at the operator guide rather than the project README, which is what the
# label is defined to carry; image.url is the one that points at the project. It is pinned to the
# revision, not to main: a label is baked in permanently, so a moving reference would rot the moment
# the file is renamed or moved, and an operator inspecting an old image wants the guide describing
# that image's configuration keys, not a later one's. The revision always resolves on GitHub, whereas
# a tag reference would not for snapshot and CI builds.
ARG IMAGE_VERSION
ARG IMAGE_REVISION
LABEL org.opencontainers.image.title="ScalarDB Saga daemon" \
      org.opencontainers.image.description="Saga-based distributed transaction coordination engine, exposed over REST and gRPC." \
      org.opencontainers.image.vendor="Scalar, Inc." \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.url="https://github.com/scalar-labs/scalardb-saga" \
      org.opencontainers.image.source="https://github.com/scalar-labs/scalardb-saga" \
      org.opencontainers.image.documentation="https://github.com/scalar-labs/scalardb-saga/blob/${IMAGE_REVISION}/daemon/docker/README.md" \
      org.opencontainers.image.version="${IMAGE_VERSION}" \
      org.opencontainers.image.revision="${IMAGE_REVISION}"
