# syntax=docker/dockerfile:1.2
FROM python:3.8-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=.

RUN --mount=type=cache,target=/root/.cache/ \
    pip install poetry

COPY pyproject.toml poetry.lock ./

RUN --mount=type=cache,target=/root/.cache/pypoetry/cache/ \
    poetry install --no-root -E server
COPY . .

ENTRYPOINT [ "poetry", "run" ]
