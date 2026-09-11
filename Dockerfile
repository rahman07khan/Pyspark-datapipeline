FROM python:3.10-slim-bookworm

LABEL maintainer="PySpark Setup"
LABEL description="Docker image for PySpark with Spark Master and Worker support"

# Install Java, procps, and required utilities.
# procps provides the ps command required by Spark startup scripts.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jdk \
        procps \
        curl \
        wget \
        ca-certificates \
        bash \
    && rm -rf /var/lib/apt/lists/*

# Debian's default-jdk package creates this symlink.
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install Spark
ENV SPARK_VERSION=3.4.0
ENV HADOOP_VERSION=3

RUN wget -q \
    https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    -O /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm -f /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark

ENV PATH=${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}

ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools && \
    pip install --no-cache-dir \
        pyspark==3.4.0 \
        pandas \
        numpy \
        pyarrow

WORKDIR /app

RUN mkdir -p /app/data /app/output /app/logs

# Verify the installation during image build
RUN test -x "${JAVA_HOME}/bin/java" && \
    java -version && \
    python --version && \
    ps --version && \
    "${SPARK_HOME}/bin/spark-submit" --version

EXPOSE 8080 7077 8081 4040

CMD ["/bin/bash"]