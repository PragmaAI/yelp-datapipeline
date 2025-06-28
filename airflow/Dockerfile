FROM python:3.10

RUN apt-get update && \
    apt-get install -y curl build-essential libssl-dev pkg-config git && \
    curl https://sh.rustup.rs -sSf | sh -s -- -y

ENV PATH="/root/.cargo/bin:$PATH"
ENV AIRFLOW_HOME=/app/airflow

WORKDIR /app

COPY dags/ dags/
COPY scripts/ scripts/
COPY airflow/entrypoint.sh entrypoint.sh
COPY requirements.txt requirements.txt

RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip install apache-airflow==2.8.0 --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-3.10.txt

RUN chmod +x entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]
