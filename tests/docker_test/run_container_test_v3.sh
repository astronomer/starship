#!/usr/bin/env bash

IMAGE=$1
WHEEL=$2

echo -e "[STARSHIP-INSTALL-START image=$IMAGE]"
pushd /usr/local/airflow/
INSTALL_OUTPUT=$( \
  python -m pip install --upgrade pip && \
  python -m pip install pytest $2 2>&1 \
)
echo -e $INSTALL_OUTPUT
popd

# if we don't find 'Successfully installed' or if we find 'ERROR'
grep -q 'Successfully installed astronomer-starship' <<< $INSTALL_OUTPUT
SUCCESSFULLY_INSTALLED=$?
grep -q 'ERROR' <<< $INSTALL_OUTPUT
ERROR=$?
if [ $SUCCESSFULLY_INSTALLED -eq 0 ] && [ $ERROR -ne 0 ]; then
  echo -e "[STARSHIP-INSTALL-SUCCESS image=$IMAGE]"
else
  echo -e "[STARSHIP-INSTALL-ERROR image=$IMAGE]"
  exit 1
fi

echo -e "[STARSHIP-COPY-DAG-START image=$IMAGE]"
mkdir -p dags || exit 1
cp /usr/local/airflow/starship/tests/docker_test/dag.py dags || exit 1
ls -R dags
echo -e "[STARSHIP-COPY-DAG-SUCCESS image=$IMAGE]"

echo "[STARSHIP-DB-INIT-START image=$IMAGE]"
airflow db migrate || exit 1
echo -e "[STARSHIP-DB-INIT-SUCCESS image=$IMAGE]"

echo "[STARSHIP-AIRFLOW-STARTUP-START image=$IMAGE]"
touch airflow.log
touch airflow.scheduler.log
airflow api-server --workers 1 2>&1 | tee -a airflow.log &
airflow scheduler 2>&1 | tee -a airflow-scheduler.log &

( timeout --signal=SIGINT 300 tail -f -n0 airflow.log & ) | grep -q "Uvicorn running on http://0.0.0.0:8080"
if [ $? -eq 0 ]; then
  echo -e "[STARSHIP-AIRFLOW-STARTUP-SUCCESS image=$IMAGE]"
else
  echo -e "[STARSHIP-AIRFLOW-STARTUP-ERROR image=$IMAGE]"
  exit 1
fi

echo -e "[STARSHIP-AIRFLOW-DAGBAG-START image=$IMAGE]"
python -c "from airflow.models.dagbag import DagBag; print(DagBag().dags)"
if [ $? -eq 0 ]; then
  echo -e "[STARSHIP-AIRFLOW-DAGBAG-SUCCESS image=$IMAGE]"
else
  echo -e "[STARSHIP-AIRFLOW-DAGBAG-ERROR image=$IMAGE]"
  exit 1
fi

echo -e "[STARSHIP-PYTEST-START image=$IMAGE]"
pytest \
  --no-header --disable-warnings --tb=short --strict-markers \
  /usr/local/airflow/starship/tests/docker_test/docker_test.py
if [ $? -eq 0 ]; then
  echo -e "[STARSHIP-PYTEST-SUCCESS image=$IMAGE]"
else
  echo -e "[STARSHIP-PYTEST-ERROR image=$IMAGE]"
  exit 1
fi

#standalone | Airflow is ready
# webserver | [2024-02-08 23:22:33 +0000] [3228] [INFO] Listening at: http://0.0.0.0:8080 (3228)
