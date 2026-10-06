# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.11.16 AS uv

FROM python:3.12.12-slim-bookworm AS builder
COPY --from=uv /uv /uvx /bin/

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

FROM python:3.12.12-slim-bookworm AS runtime
RUN groupadd --gid 1000 app \
    && useradd --uid 1000 --gid app --create-home app \
    && mkdir /data \
    && chown app:app /data

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY --from=builder --chown=app:app /app/.venv /app/.venv
COPY --chown=app:app app ./app

USER app
EXPOSE 8000
CMD ["fastapi", "run", "app/main.py", "--host", "0.0.0.0", "--port", "8000"]
