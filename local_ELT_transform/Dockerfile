FROM python:3.10-slim-buster

RUN apt-get update 

WORKDIR /app

COPY . /app

COPY profiles.yml /root/.dbt/

RUN pip install --no-cache-dir dbt-postgres

ENTRYPOINT [ "dbt" ]