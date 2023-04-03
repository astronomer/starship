
# Deploying astronomer-starship
On the `master` branch, in the `astronomer-starship` directory:
- Update the version in `setup.cfg` following [semver standards](https://semver.org/)
- Run `make tag`

# Deploying astronomer-starship-provider
On the `master` branch, in the `astronomer-starship-provider` directory:
- Update the version in `setup.cfg` following [semver standards](https://semver.org/)
- Run `make tag`

# Easier Plugin Development
1. create a new project `astro dev init`
2. edit `Dockerfile`
```
FROM quay.io/astronomer/astro-runtime:7.3.0

ENV AIRFLOW__WEBSERVER__RELOAD_ON_PLUGIN_CHANGE=true
ENV FLASK_ENV=development
ENV FLASK_DEBUG=true

COPY --chown=astro:astro --chmod=777 lib lib
COPY --chown=astro:astro --chmod=777 starship/astronomer-starship astronomer-starship
RUN pip install -e astronomer-starship

COPY --chown=astro:astro --chmod=777 starship/astronomer-starship-provider astronomer-starship-provider
RUN pip install -e astronomer-starship-provider
```
3. create `docker-compose.override.yml` with
```
version: "3.1"
services:
  webserver:
    volumes:
      - ./starship/astronomer-starship:/usr/local/airflow/astronomer-starship:rw
      - ./starship/astronomer-starship-provider:/usr/local/airflow/astronomer-starship-provider:rw
    command: >
      bash -c 'if [[ -z "$$AIRFLOW__API__AUTH_BACKEND" ]] && [[ $$(pip show -f apache-airflow | grep basic_auth.py) ]];
        then export AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.basic_auth ;
        else export AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.default ; fi &&
        { airflow users create "$$@" || airflow create_user "$$@" ; } &&
        { airflow sync-perm || airflow sync_perm ;} &&
        airflow webserver -d' -- -r Admin -u admin -e admin@example.com -f admin -l user -p admin
```
3. Link starship locally `ln -s ~/starship starship` (run this once)
4. Build with `(tar -czh . | docker build -t local -)` (run this many times)
5. Run with `astro dev start -i local` 

**note:** that we have `astronomer-starship` mounted into our image via `docker-compose.override.yml`, so we don't need to always rebuild. We also have various flask/airflow debug and plugin reload settings enabled.

**note**: alternative, possibly easier instructions here - https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html#troubleshooting

## Restarting just the Webserver
Not all changes take effect on just a page reload, you can restart the webserver without restarting the start:
```shell
docker restart $(docker container ls --filter name=webserver --format="{{.ID}}")
```
Sometimes this is effective to invalidate the cache