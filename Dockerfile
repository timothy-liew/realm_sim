FROM apache/airflow:2.9.0-python3.10

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY src /opt/airflow/src
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
