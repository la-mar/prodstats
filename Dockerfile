

FROM segment/chamber:2.7.5 as build

FROM python:3.8.1 as base

LABEL "com.datadoghq.ad.logs"='[{"source": "python","service": "prodstats"}]'


ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_VERSION=1.0

ENV PYTHONPATH=/app/prodstats

# Install Poetry & ensure it is in $PATH
RUN pip install "poetry==$POETRY_VERSION"
ENV PATH "/root/.poetry/bin:/opt/venv/bin:/:${PATH}"

# copy only requirements to cache them in separate layer
WORKDIR /app
COPY poetry.lock pyproject.toml /app/

# project initialization
RUN poetry install --no-dev --no-interaction

RUN mkdir /app/prodstats && touch /app/prodstats/__init__.py

# install again to source symlinked application in first call to "poetry install"
RUN poetry install --no-dev --no-interaction

# copy project files
COPY ./src /app
COPY ./config /app/config
COPY ./alembic.ini /app/alembic.ini

# rewrite path to migration dir since it differs in the container
RUN sed -i 's/\/src//' alembic.ini

# create unprivileged user
RUN groupadd -r celeryuser && useradd -r -m -g celeryuser celeryuser
RUN find /app ! -user celeryuser -exec chown celeryuser {} \;
RUN find /app/prodstats ! -user celeryuser -exec chown celeryuser {} \;

COPY --from=build /chamber /chamber
