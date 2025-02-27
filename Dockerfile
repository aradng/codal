FROM python:3.12-slim

ENV TZ=UTC \
    PYTHONUNBUFFERED=1 \
    PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_VERSION=2.1.1 \
    DAGSTER_HOME=/opt/dagster

RUN pip --retries 10 install --upgrade pip setuptools wheel
RUN pip --retries 10 install poetry

COPY pyproject.toml poetry.lock README.md ./
RUN poetry install --without dev --no-root --no-directory --compile

RUN mkdir -p $DAGSTER_HOME
WORKDIR $DAGSTER_HOME
# COPY dagster.yaml workspace.yaml .

# COPY . $DAGSTER_HOME/app
