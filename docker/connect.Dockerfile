# syntax=docker/dockerfile:1
# Debezium Connect plus Confluent's Protobuf converter, which the Debezium image does not ship.
# The converter is downloaded at build time (pinned version and checksum), not vendored: it is
# distributed under the Confluent Community License and Apache 2.0.

ARG PROTOBUF_CONVERTER_VERSION=8.3.2

FROM python:3.12.15-slim-bookworm AS protobuf-converter
ARG PROTOBUF_CONVERTER_VERSION
ADD --checksum=sha256:5617917614455c6af7f28bbf001daf0b24e1d2f24141ff9183db89540450cb4b \
    https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-protobuf-converter/versions/${PROTOBUF_CONVERTER_VERSION}/confluentinc-kafka-connect-protobuf-converter-${PROTOBUF_CONVERTER_VERSION}.zip \
    /tmp/converter.zip
RUN python -m zipfile -e /tmp/converter.zip /tmp/converter \
    && mv /tmp/converter/confluentinc-kafka-connect-protobuf-converter-${PROTOBUF_CONVERTER_VERSION}/lib /tmp/lib

FROM quay.io/debezium/connect:3.7.0.Final
# A directory of its own under the plugin path: Connect loads it in an isolated class loader,
# so its protobuf-java and Kotlin jars cannot clash with the Debezium connectors.
COPY --from=protobuf-converter /tmp/lib/ /kafka/connect/confluent-protobuf-converter/
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=30 \
    CMD curl -fsS http://localhost:8083/connectors >/dev/null || exit 1
