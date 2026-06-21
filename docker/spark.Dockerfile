FROM python:3.13-slim

# Install Java 17 runtime (Delta Lake 4.1 requires Java 17)
# and curl for downloading JARs during the build.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Tell Spark where Java lives.
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Install PySpark.
RUN pip install --no-cache-dir pyspark==4.1.2

# Download Delta Lake and Postgres JDBC JARs.
# These live in a known location that session.py will reference.
ENV SPARK_EXTRA_JARS=/opt/spark-extra-jars
RUN mkdir -p ${SPARK_EXTRA_JARS} && \
    curl -L -o ${SPARK_EXTRA_JARS}/delta-spark.jar \
        https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.1.0/delta-spark_2.13-4.1.0.jar && \
    curl -L -o ${SPARK_EXTRA_JARS}/delta-storage.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/4.1.0/delta-storage-4.1.0.jar && \
    curl -L -o ${SPARK_EXTRA_JARS}/postgresql.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

# Working directory for jobs.
WORKDIR /opt/airflow

# Idle by default — we exec into the container to run jobs.
CMD ["sleep", "infinity"]