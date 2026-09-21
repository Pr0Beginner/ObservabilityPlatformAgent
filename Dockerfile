FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY contracts ./contracts

RUN pip install --no-cache-dir .

RUN adduser --disabled-password --gecos "" --uid 10001 agent \
    && mkdir -p /app/.data \
    && chown -R agent:agent /app

USER agent

EXPOSE 8090

HEALTHCHECK --interval=30s --timeout=3s --start-period=15s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8090/health/ready', timeout=2)"]

CMD ["observability-agent"]
