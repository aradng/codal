FROM python:3.12-bullseye

ENV TZ=UTC \
    PYTHONUNBUFFERED=1 \
    PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_VERSION=1.8.2

RUN pip --retries 10 install --upgrade pip setuptools wheel
RUN pip --retries 10 install poetry

COPY pyproject.toml poetry.lock ./
RUN poetry install --without dev

RUN mkdir -p /opt/dagster
ENV DAGSTER_HOME=/opt/dagster