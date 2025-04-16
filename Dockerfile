FROM apache/airflow:2.9.0-python3.10

# Set environment variables for Spark
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYSPARK_PYTHON=python3

# Install Java and procps for Spark compatibility
USER root
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    procps \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY src /opt/airflow/src
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
