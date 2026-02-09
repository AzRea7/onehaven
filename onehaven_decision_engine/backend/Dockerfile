FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# ✅ make sure python can always import "app"
ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["bash", "-lc", "alembic -c app/alembic.ini upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000"]
