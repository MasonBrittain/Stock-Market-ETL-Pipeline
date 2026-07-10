# Build context is the repository root (set in docker-compose.streaming.yml)
FROM bitnami/spark:3.5

USER root
RUN pip install --no-cache-dir "pandas>=2.0" "SQLAlchemy>=2.0" "pyarrow>=15.0"

WORKDIR /app
COPY streaming/common/ ./common/
COPY streaming/processor/processor.py .
COPY src/ ./src/

# spark-sql-kafka is fetched from Maven Central on first start and cached.
CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "/app/processor.py"]
