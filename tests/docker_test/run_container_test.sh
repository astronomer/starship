#!/usr/bin/env bash

IMAGE=$1
WHEEL=$2

AIRFLOW_MAJOR_VERSION=$(airflow version | grep -oE '[0-9]+' | head -1)

if [ "$AIRFLOW_MAJOR_VERSION" != "2" ] && [ "$AIRFLOW_MAJOR_VERSION" != "3" ]; then
  echo "Unsupported Airflow version: $AIRFLOW_MAJOR_VERSION"
  exit 1
fi

echo -e "[STARSHIP-INSTALL-START image=$IMAGE]"
pushd /usr/local/airflow/ || exit 1
INSTALL_OUTPUT=$( \
  python -m pip install --upgrade pip && \
  python -m pip install pytest $WHEEL 2>&1 \
)
echo -e $INSTALL_OUTPUT
popd || exit 1

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
if [ "$AIRFLOW_MAJOR_VERSION" = "2" ]; then
  airflow db init || exit 1
elif [ "$AIRFLOW_MAJOR_VERSION" = "3" ]; then
  airflow db migrate || exit 1
fi
echo -e "[STARSHIP-DB-INIT-SUCCESS image=$IMAGE]"

echo "[STARSHIP-AIRFLOW-STARTUP-START image=$IMAGE]"
touch airflow.log
touch airflow.scheduler.log
if [ "$AIRFLOW_MAJOR_VERSION" = "2" ]; then
  airflow webserver --workers 1 2>&1 | tee -a airflow.log &
  http_server_ready_line="Listening at: http://0.0.0.0:8080"
  dagbag_print_command="print(DagBag(store_serialized_dags=True).dags)"
elif [ "$AIRFLOW_MAJOR_VERSION" = "3" ]; then
  airflow api-server --workers 1 2>&1 | tee -a airflow.log &
  http_server_ready_line="Uvicorn running on http://0.0.0.0:8080"
  dagbag_print_command="print(DagBag().dags)"
fi
airflow scheduler 2>&1 | tee -a airflow-scheduler.log &

( timeout --signal=SIGINT 300 tail -f -n0 airflow.log & ) | grep -q "$http_server_ready_line"
if [ $? -eq 0 ]; then
  echo -e "[STARSHIP-AIRFLOW-STARTUP-SUCCESS image=$IMAGE]"
else
  echo -e "[STARSHIP-AIRFLOW-STARTUP-ERROR image=$IMAGE]"
  exit 1
fi

echo -e "[STARSHIP-AIRFLOW-DAGBAG-START image=$IMAGE]"
python -c "from airflow.models.dagbag import DagBag; $dagbag_print_command"
if [ $? -eq 0 ]; then
  echo -e "[STARSHIP-AIRFLOW-DAGBAG-SUCCESS image=$IMAGE]"
else
  echo -e "[STARSHIP-AIRFLOW-DAGBAG-ERROR image=$IMAGE]"
  exit 1
fi

# For AF3, manually reserialize DAGs to database (scheduler takes too long)
if [ "$AIRFLOW_MAJOR_VERSION" = "3" ]; then
  echo -e "[STARSHIP-DAG-RESERIALIZE-START image=$IMAGE]"
  airflow dags reserialize 2>&1 | grep -v graphviz
  if [ $? -eq 0 ]; then
    echo -e "[STARSHIP-DAG-RESERIALIZE-SUCCESS image=$IMAGE]"
  else
    echo -e "[STARSHIP-DAG-RESERIALIZE-ERROR image=$IMAGE]"
  fi
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
