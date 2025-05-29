ARG py_version=3.11.2

FROM python:$py_version-slim-bullseye as base

RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    build-essential=12.9 \
    ca-certificates=20210119 \
    git=1:2.30.2-1+deb11u2 \
    libpq-dev=13.21-0+deb11u1 \
    make=4.3-4.1 \
    openssh-client=1:8.4p1-5+deb11u3 \
    software-properties-common=0.96.20.2-2.1 \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

RUN python -m pip install --upgrade "pip==24.0" "setuptools==69.2.0" "wheel==0.43.0" --no-cache-dir

ARG commit_ref=main

RUN python -m pip install --no-cache-dir dbt-core

RUN python -m pip install --no-cache-dir dbt-postgres

WORKDIR /projeto_dbt

COPY projeto_dbt .

COPY profiles.yml .

# COPY exemplo_vendas.csv /projeto_dbt/seeds/

EXPOSE 8080 

# Expose port 8080 for dbt server

# RUN dbt seed --profiles-dir .

CMD dbt deps && dbt build --profiles-dir . && dbt serve docs && sleep infinity
