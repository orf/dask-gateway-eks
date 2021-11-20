# syntax=docker/dockerfile:1.2
FROM python:3.8

ENV PYTHONDONTWRITEBYTECODE=1

RUN --mount=type=cache,target=/root/.cache/ \
    pip install poetry

COPY pyproject.toml poetry.lock ./

RUN --mount=type=cache,target=/root/.cache/pypoetry/cache/ \
    poetry install --no-root
COPY . .
RUN poetry install

ENTRYPOINT [ "poetry", "run" ]
