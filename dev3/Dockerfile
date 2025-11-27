ARG RUNTIME_VERSION=3.1-5
FROM astrocrpublic.azurecr.io/runtime:${RUNTIME_VERSION}-base

WORKDIR /usr/local/airflow
USER root

COPY dev3/packages.txt .
RUN apt-get update && cat packages.txt | xargs apt-get install -y && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean

COPY dev3/requirements.txt .
RUN pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

COPY --chown=astro:astro dist/*.whl /usr/local/airflow/
RUN pip install --no-cache-dir ./*.whl

USER astro
