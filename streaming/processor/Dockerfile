# Build context is the repository root (set in docker-compose.streaming.yml)
FROM apache/spark:3.5.1-scala2.12-java11-python3-ubuntu

USER root
RUN pip3 install --no-cache-dir "pandas>=2.0" "SQLAlchemy>=2.0" "pyarrow>=15.0"

WORKDIR /app
COPY streaming/common/ ./common/
COPY streaming/processor/processor.py .
COPY src/ ./src/

# spark-sql-kafka is fetched from Maven Central on first start and cached.
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "/app/processor.py"]
