FROM apache/airflow

ARG AIRFLOW_VERSION=2.9.3

COPY docker-requirements.txt ${AIRFLOW_HOME}/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r ${AIRFLOW_HOME}/requirements.txt

COPY . ${AIRFLOW_HOME}/dags/

COPY .env.default ${AIRFLOW_HOME}/.env

RUN rm ${AIRFLOW_HOME}/dags/.env.default
RUN rm ${AIRFLOW_HOME}/dags/docker-requirements.txt
