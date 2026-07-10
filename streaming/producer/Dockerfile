# Build context is the repository root (set in docker-compose.streaming.yml)
FROM python:3.11-slim

WORKDIR /app

COPY streaming/producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streaming/common/ ./common/
COPY streaming/producer/producer.py .

CMD ["python", "producer.py"]
