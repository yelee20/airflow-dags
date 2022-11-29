FROM --platform=linux/amd64 apache/airflow:2.3.3-python3.8
USER root
LABEL maintainer="yewon"

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    ca-certificates curl firefox-esr           \
 && rm -fr /var/lib/apt/lists/*                \
 && curl -L https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz | tar xz -C /usr/local/bin \
 && apt-get purge -y ca-certificates curl

COPY ./dags /opt/airflow/dags

USER airflow

RUN pip install --upgrade pip
COPY requirements.txt ./requirements.txt
RUN pip install --user -r requirements.txt



