
ARG py_version=3.11.2
FROM python:$py_version-slim-bullseye AS base


RUN apt-get update \
 && apt-get install -y --no-install-recommends \
   build-essential \
   ca-certificates \
   git \
   libpq-dev \
   make \
   openssh-client \
   software-properties-common \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8


RUN python -m pip install --upgrade "pip==24.0" "setuptools==69.2.0" "wheel==0.43.0" --no-cache-dir


WORKDIR /usr/app/dbt_project


COPY ./dbt/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./dbt/projeto_dbt .

EXPOSE 8080


CMD [ "tail", "-f", "/dev/null" ]