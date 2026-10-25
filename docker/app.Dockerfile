# syntax=docker/dockerfile:1
# Three runtime targets from one lock file: `mutator` (no JVM), `spark` (JRE, pyspark, baked jars)
# and `analytics` (dbt, DuckDB with its extensions, Streamlit; no JVM).

FROM ghcr.io/astral-sh/uv:0.13.5 AS uv
FROM eclipse-temurin:17.0.20.1_1-jre-noble AS jre

FROM python:3.12.15-slim-bookworm AS build-base
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /app

# Dependencies first: this layer only changes when pyproject.toml or uv.lock do.
FROM build-base AS build-mutator
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --locked --no-dev --no-editable --no-install-project
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

FROM build-base AS build-spark
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --locked --no-dev --no-editable --no-install-project --extra spark
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --extra spark

FROM build-base AS build-analytics
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --locked --no-dev --no-editable --no-install-project --extra analytics
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --extra analytics

# Same base as the builders, so the venv's interpreter path and ABI match.
FROM python:3.12.15-slim-bookworm AS runtime-base
RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --create-home app \
    && mkdir /runs \
    && chown app:app /runs
ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1
WORKDIR /home/app

FROM runtime-base AS mutator
COPY --from=build-mutator /opt/venv /opt/venv
USER app
# One-shot CLI: it exits when the mutations are done, so a health probe has nothing to report.
HEALTHCHECK NONE
ENTRYPOINT ["cdc-demo"]

FROM runtime-base AS spark
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_IVY_DIR=/opt/spark-ivy
ENV PATH="${JAVA_HOME}/bin:${PATH}"
COPY --from=build-spark /opt/venv /opt/venv
RUN java -version \
    && mkdir /opt/spark-ivy \
    && chown app:app /opt/spark-ivy
USER app
# Resolve the Spark packages now so containers never need Maven access at run time.
RUN cdc-demo spark-warmup
# No curl in the image; the Spark UI answering on 4040 means the session is up.
HEALTHCHECK --interval=15s --timeout=5s --start-period=90s --retries=5 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:4040/', timeout=4)"]
ENTRYPOINT ["cdc-demo"]

FROM runtime-base AS analytics
COPY --from=build-analytics /opt/venv /opt/venv
COPY dbt /opt/dbt
COPY dashboard /opt/dashboard
ENV DBT_PROJECT_DIR=/opt/dbt \
    DBT_PROFILES_DIR=/opt/dbt \
    WAREHOUSE_DIR=/warehouse
RUN mkdir /warehouse \
    && chown app:app /warehouse
USER app
# Fetch the DuckDB extensions now (into the app user's home) so containers never need internet
# access at run time; LOAD proves they match the installed DuckDB.
RUN python -c "import duckdb; c = duckdb.connect(); [c.execute(q) for e in ('httpfs', 'delta') for q in (f'INSTALL {e}', f'LOAD {e}')]"
# The dashboard service sets its own probe; build-marts is a short-lived command.
HEALTHCHECK NONE
ENTRYPOINT ["cdc-demo"]
