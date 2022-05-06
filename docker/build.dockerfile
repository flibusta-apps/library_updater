FROM ghcr.io/kurbezz/base_docker_images:3.10-postgres-asyncpg-poetry-buildtime as build-image

WORKDIR /root/poetry
COPY pyproject.toml poetry.lock /root/poetry/

ENV VENV_PATH=/opt/venv
RUN poetry export --without-hashes > requirements.txt \
    && . "${VENV_PATH}/bin/activate" \
    && pip install -r requirements.txt --no-cache-dir


FROM ghcr.io/kurbezz/base_docker_images:3.10-postgres-runtime as runtime-image

RUN apt-get update \
    && apt-get install --no-install-recommends -y wget default-mysql-client-core \
    && rm -rf /var/lib/apt/lists/*

ENV VENV_PATH=/opt/venv
ENV PATH="$VENV_PATH/bin:$PATH"

WORKDIR /app/

COPY ./src/ /app/
COPY --from=build-image $VENV_PATH $VENV_PATH
COPY ./scripts/healthcheck.py /root/

EXPOSE 8080

CMD gunicorn -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:8080
