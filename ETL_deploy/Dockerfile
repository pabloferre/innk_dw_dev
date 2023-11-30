# Use an official Python 3.11 image as a base
FROM python:3.11-slim-buster

# Set environment variables
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW_UID=50000

# Install dependencies and Apache Airflow
RUN apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        build-essential \
        gosu \
    && apt-get purge --auto-remove -yqq build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* 



# Create Airflow user and setup directories
RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} -u ${AIRFLOW_UID} airflow

# Copy the requirements file and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt 



COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Setup the Airflow directories with the correct permissions
RUN mkdir -p ${AIRFLOW_HOME}/dags \
    && mkdir -p ${AIRFLOW_HOME}/logs \
    && mkdir -p ${AIRFLOW_HOME}/plugins \
    && chown -R airflow: ${AIRFLOW_HOME}

# Copy over the Airflow configuration and DAGs
COPY --chown=airflow ./dags ${AIRFLOW_HOME}/dags
COPY --chown=airflow ./plugins ${AIRFLOW_HOME}/plugins
COPY --chown=airflow ./airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY --chown=airflow ./logs /opt/airflow/logs/

USER airflow
ENTRYPOINT ["/entrypoint.sh"]

# Expose the default webserver and worker ports
EXPOSE 8080
EXPOSE 8793


