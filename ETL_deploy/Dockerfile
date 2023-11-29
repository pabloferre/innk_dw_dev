# Use an official Python 3.11 image as a base
FROM python:3.11-slim-buster

# Set environment variables
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW_UID=50000
COPY requirements.txt /requirements.txt

# Install dependencies and Apache Airflow
RUN apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        build-essential \
        gosu \
    #&& pip install apache-airflow==2.7.2 \
    && apt-get purge --auto-remove -yqq build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* 



# Create Airflow user and setup directories
RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow 
# Switch to the Airflow user


RUN apt-get update && apt-get install -y dumb-init && apt-get clean && rm -rf /var/lib/apt/lists/*



COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN echo "airflow:x:50000:50000:airflow:/usr/local/airflow:/bin/bash" >> /etc/passwd
RUN echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
RUN chown -R airflow:airflow /usr/local/airflow/



RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt 

RUN mkdir -p ${AIRFLOW_HOME}/dags \
    && mkdir -p ${AIRFLOW_HOME}/logs \
    && chown -R airflow: ${AIRFLOW_HOME} 

COPY --chown=airflow . ${AIRFLOW_HOME}
COPY --chown=airflow ./dags ${AIRFLOW_HOME}/dags
COPY --chown=airflow ./plugins ${AIRFLOW_HOME}/plugins
COPY --chown=airflow ./logs ${AIRFLOW_HOME}/logs
COPY --chown=airflow ./airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY --chown=airflow ./logs /opt/airflow/logs/

USER airflow
ENTRYPOINT ["/entrypoint.sh"]

# Expose the default webserver and worker ports
EXPOSE 8080
EXPOSE 8793


