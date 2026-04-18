FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    ca-certificates \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpangocairo-1.0-0 \
    libpango-1.0-0 \
    libcairo2 \
    libx11-6 \
    libxcb1 \
    libxext6 \
    libx11-xcb1 \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m playwright install chromium

COPY app ./app

ENV PYTHONPATH=/app
ENV POLICY_PLAYWRIGHT_ENABLED=1
ENV POLICY_PLAYWRIGHT_TIMEOUT_MS=25000
ENV POLICY_FETCH_CACHE_DIR=/tmp/policy_fetch_cache

EXPOSE 8000

CMD ["bash", "-lc", "alembic -c app/alembic.ini upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000"]
