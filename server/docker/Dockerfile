# syntax=docker/dockerfile:1

# Build context is server/build/docker, assembled by the `dockerContext` Gradle task — not this
# directory and not the repository root. Build it with `./gradlew :server:dockerBuild` locally, or
# via docker/build-push-action in the release workflow. The distribution is built by Gradle on the
# host and only packaged here: building it inside a stage instead would re-resolve every dependency
# on each image build and lose the build cache CI already has.

# Tag and digest together: the tag documents what this is, the digest is what actually gets pulled, so
# rebuilding a commit yields the same base layer and a silently-retagged upstream cannot change what
# ships. Dependabot updates both (see .github/dependabot.yml).
#
# A JRE, not a JDK: the daemon runs a start script and needs no compiler, so shipping javac would add
# tooling that is useful to an attacker inside the container and to nobody else.
#
# An LTS major, and 25 specifically: the engine runs sagas on virtual threads, and JEP 491 (JDK 24) is
# what keeps a JDBC driver's monitor from pinning the carrier for the length of each protocol
# exchange; while it pins, effective database concurrency is capped at the number of carriers. Interim
# releases stop receiving patches the moment the next one ships, so only LTS majors are candidates.
FROM eclipse-temurin:25-jre-noble@sha256:b4c93a50fc67612798db73d68ca3b0ee4ebdd51736e59cca370e689b9797037e

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
 && groupadd --system --gid 201 scalardb \
 && useradd --system --uid 201 --gid 201 --no-create-home scalardb

WORKDIR /scalardb-saga

# Both trees below stay owned by root rather than by uid 201, so do not add --chown. The daemon never
# writes to disk; logging goes to stdout, and the configuration and definitions are read rather than
# written, so the runtime user needs read and execute and nothing more. Root ownership then denies a
# compromised process the cheapest way to persist: rewriting the start script or dropping a jar into
# lib/ so the next restart runs it. It cannot chown its way back either, since a non-root user holds
# no CAP_CHOWN. Modes come from the build context, where installDist yields 0755 directories and
# scripts and 0644 jars; image-smoke-test boots the container as 201, so a build that made them
# unreadable fails CI rather than shipping.

# The `installDist` tree, not the distribution tar: the tar's internal root directory embeds the
# version (scalardb-saga-server-1.2.3/), which would make every path in this file change each
# release. Copied before the configuration so that editing the config does not invalidate the ~150 MB
# jar layer.
COPY dist/ ./

# A template, not a working configuration: the image cannot serve anything until an operator supplies
# real ScalarDB connection settings and at least one saga definition. Mount over /scalardb-saga/conf
# (e.g. a ConfigMap plus a definitions volume) or derive an image that bakes them in.
COPY conf/ ./conf/

# The product license plus generated attributions for every third-party jar vendored into the layer
# above. /licenses is where the Red Hat/OpenShift convention looks, and a predictable location
# regardless of platform.
COPY LICENSE THIRD-PARTY-NOTICES.txt /licenses/

USER 201:201

# The SagaServerConfig defaults: 12080 REST, 12051 gRPC. Both transports are on by default and either
# can be disabled in the configuration, in which case its port simply goes unbound.
EXPOSE 12080 12051

# No entrypoint wrapper script. The JVM flags (MaxRAMPercentage, ExitOnOutOfMemoryError,
# enable-native-access) are baked into the start script by Gradle's applicationDefaultJvmArgs, and
# JAVA_OPTS still overrides them at run time — so the shell hop the other Scalar images use to assemble JAVA_OPTS
# buys nothing here. The script ends in `exec java`, so the JVM becomes PID 1 and receives SIGTERM
# directly; SagaServer's shutdown hook then drains in-flight sagas rather than dropping them.
#
# No grpc_health_probe binary either: Kubernetes has had native gRPC probes since 1.24 (GA in 1.27),
# and the daemon serves both `GET /health` and the standard grpc.health.v1.Health service — the
# latter deliberately left unauthenticated so a gRPC-only deployment stays probeable.
ENTRYPOINT ["/scalardb-saga/bin/scalardb-saga-server"]
CMD ["--config", "/scalardb-saga/conf/server.properties"]

# Declared last: these change on every release, and anything below a changed instruction is rebuilt.
#
# The same label set ScalarDB Cluster and the other Scalar server images carry, from the same Red Hat
# container conventions as the /licenses directory above. No build timestamp among them, deliberately:
# a label that moved on every rebuild would give identical inputs a different digest. Buildx records
# the build time in provenance attestation instead.
#
# `release` carries the git revision. The convention wants whatever distinguishes two builds of one
# version, and this repository has no build-release counter — the commit is that thing. Both args
# default so a bare `docker build` of an assembled context still yields a value; every image built
# through `:server:dockerBuild` or CI passes the real ones and never falls back to these.
ARG IMAGE_VERSION=unspecified
ARG IMAGE_REVISION=main
LABEL name="scalardb-saga-server" \
      maintainer="Scalar, Inc." \
      vendor="Scalar, Inc." \
      version="${IMAGE_VERSION}" \
      release="${IMAGE_REVISION}" \
      summary="ScalarDB Saga Server Container Image" \
      description="ScalarDB Saga is a modern saga orchestration engine for microservices. It coordinates eventually consistent distributed transactions across services using the Saga pattern (steps with compensations) and TCC (try/confirm/cancel), keeping saga state durable in a database of your choice — with no message broker to operate."
