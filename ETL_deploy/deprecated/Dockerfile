FROM python:3.11-slim-buster

# Install OS-level dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set Airflow home and web server port
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW__WEBSERVER__BASE_URL=http://localhost:8080
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080

# Install Airflow with required extras
RUN pip install --upgrade pip
RUN pip install apache-airflow[postgres,crypto,celery,redis,kubernetes]==2.7.2  # Adjust version if needed

# Install additional dependencies for your Python script
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Setup Airflow user and directories
RUN useradd --create-home --home-dir $AIRFLOW_HOME airflow \
    && chown -R airflow: $AIRFLOW_HOME \
    && mkdir -p $AIRFLOW_HOME/dags $AIRFLOW_HOME/logs \
    && chown -R airflow: $AIRFLOW_HOME/dags $AIRFLOW_HOME/logs 

RUN mkdir -p $AIRFLOW_HOME/logs && chown -R airflow: $AIRFLOW_HOME/logs

EXPOSE 8080

USER airflow
WORKDIR $AIRFLOW_HOME

RUN pip install apache-airflow[cncf.kubernetes]
