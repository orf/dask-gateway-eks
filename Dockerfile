FROM python:3.8
RUN pip install poetry
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root
COPY . .
RUN poetry install

ENTRYPOINT [ "poetry", "run" ]
