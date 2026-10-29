FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    EMAIL_SYNC_DATA_DIR=/data

WORKDIR /app
COPY pyproject.toml README.md ./
COPY emailsync ./emailsync
RUN pip install --no-cache-dir . \
    && useradd --system --uid 10001 --create-home email-sync \
    && mkdir -p /data \
    && chown email-sync:email-sync /data

USER email-sync
VOLUME ["/data"]
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2).read()"
CMD ["uvicorn", "emailsync.web.app:app", "--host", "0.0.0.0", "--port", "8080"]
