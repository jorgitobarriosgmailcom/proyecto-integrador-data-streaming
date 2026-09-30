FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
RUN uv sync
COPY app ./app
COPY tests ./tests
COPY data ./data
COPY scripts ./scripts
RUN mkdir -p /data /app/evidence/runtime
CMD ["uv", "run", "python", "app/pipeline.py"]
