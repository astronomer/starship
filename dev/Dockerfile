ARG RUNTIME_VERSION=13.0.0
FROM quay.io/astronomer/astro-runtime:${RUNTIME_VERSION}-base

WORKDIR /usr/local/airflow
USER root

COPY dev/packages.txt .
RUN apt-get update && cat packages.txt | xargs apt-get install -y && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean

COPY dev/requirements.txt .
RUN pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

COPY --chown=astro:astro dist/*.whl /usr/local/airflow/
RUN pip install --no-cache-dir ./*.whl

USER astro
