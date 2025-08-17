FROM apache/airflow:2.10.2-python3.12

# Switch to root to install system packages
USER root

# Install Java, procps, wget, curl, git
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless procps wget curl git && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Switch back to airflow user before installing Python packages
USER airflow

# Install PySpark as airflow user
RUN pip install --no-cache-dir pyspark==3.5.6
