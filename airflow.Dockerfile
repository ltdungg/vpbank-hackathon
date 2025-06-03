FROM apache/airflow:3.0.1
ARG AIRFLOW_PROJ_DIR

COPY --from=openjdk:11.0.11-jdk-slim /usr/local/openjdk-11 /usr/local/openjdk-11
ENV JAVA_HOME=/usr/local/openjdk-11
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

COPY ${AIRFLOW_PROJ_DIR}/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
