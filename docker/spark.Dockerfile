FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# arm64 (Apple Silicon) path fallback
RUN if [ ! -d "$JAVA_HOME" ]; then ln -s /usr/lib/jvm/java-17-openjdk-arm64 /usr/lib/jvm/java-17-openjdk-amd64; fi
RUN pip install --no-cache-dir pyspark==3.5.1 delta-spark==3.2.0
WORKDIR /app
