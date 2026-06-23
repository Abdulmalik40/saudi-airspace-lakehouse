FROM apache/airflow:3.2.2

COPY requirements.txt /
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.12.txt" \
    -r /requirements.txt