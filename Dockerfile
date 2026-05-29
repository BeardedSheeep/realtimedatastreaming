# syntax=docker/dockerfile:1
# Copyright (c) 2026 BeardedSheeep

FROM ghcr.io/astral-sh/uv:0.9.17-python3.12-bookworm-slim@sha256:d935373e69f9507199c29006b5eaec59893cbd606f6fb7a185da0c2c83f716e9 AS builder

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

COPY pyproject.toml uv.lock README.md LICENSE ./

RUN --mount=type=cache,id=uv-cache,target=/root/.cache/uv,sharing=locked \
    uv sync --frozen --no-dev --no-install-project

COPY realtimedatastreaming ./realtimedatastreaming

RUN --mount=type=cache,id=uv-cache,target=/root/.cache/uv,sharing=locked \
    uv sync --frozen --no-dev --no-editable

FROM python:3.14.5-slim-bookworm@sha256:a9bee15510a364124aa24692899d269835683b883de42f7ebec8c293cf679ccb

ARG OCI_CREATED="unknown"
ARG OCI_AUTHORS="BeardedSheeep"
ARG OCI_COPYRIGHT="Copyright (c) 2026 BeardedSheeep"
ARG OCI_DESCRIPTION="Realtime data streaming project scaffold."
ARG OCI_LICENSES="MIT"
ARG OCI_REF_NAME="latest"
ARG OCI_REVISION="unknown"
ARG OCI_SOURCE="https://github.com/BeardedSheeep/realtimedatastreaming"
ARG OCI_TITLE="realtimedatastreaming"
ARG OCI_URL="https://github.com/BeardedSheeep/realtimedatastreaming"
ARG OCI_VERSION="0.1.0"

LABEL org.opencontainers.image.authors="${OCI_AUTHORS}" \
      org.opencontainers.image.copyright="${OCI_COPYRIGHT}" \
      org.opencontainers.image.created="${OCI_CREATED}" \
      org.opencontainers.image.description="${OCI_DESCRIPTION}" \
      org.opencontainers.image.licenses="${OCI_LICENSES}" \
      org.opencontainers.image.ref.name="${OCI_REF_NAME}" \
      org.opencontainers.image.revision="${OCI_REVISION}" \
      org.opencontainers.image.source="${OCI_SOURCE}" \
      org.opencontainers.image.title="${OCI_TITLE}" \
      org.opencontainers.image.url="${OCI_URL}" \
      org.opencontainers.image.version="${OCI_VERSION}"

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Temporary CVE remediation for runtime packages reported by Trivy.
# Remove after the refreshed pinned python:3.12.12-slim-bookworm digest includes these fixed versions.
RUN apt-get update \
    && apt-get install -y --no-install-recommends --only-upgrade \
        libcap2=1:2.66-4+deb12u3+b1 \
        libgnutls30=3.7.9-2+deb12u7 \
        libssl3=3.0.19-1~deb12u2 \
        openssl=3.0.19-1~deb12u2 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --no-create-home --shell /usr/sbin/nologin app

COPY --from=builder --chown=app:app /app/.venv /app/.venv

USER app

ENTRYPOINT ["realtimedatastreaming"]
