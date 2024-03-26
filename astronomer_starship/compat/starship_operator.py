from typing import Literal
from logging import getLogger
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin

from airflow.models.baseoperator import BaseOperator
from airflow.utils.state import DagRunState


SOURCE_URL = "XXXXX"
ASTRO_URL = "XXXXX"
ASTRO_API_TOKEN = "XXXXX"

DAG_RUNS = "/api/starship/dag_runs"
TASK_INSTANCES = "/api/starship/task_instances"
DAGS = "/api/starship/dags"

logger = getLogger(__name__)


def session_with_retry(retries=3, backoff_factor=2):
    sess = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
    )
    sess.mount("http://", HTTPAdapter(max_retries=retry))
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    return sess


def _request(
    type: Literal["get", "post", "put", "patch"],
    endpoint,
    auth,
    json=None,
    params=None,
    retries=3,
    backoff_factor=2,
):
    s = session_with_retry(retries=retries, backoff_factor=backoff_factor)
    request_mapping = {"get": s.get, "post": s.post, "put": s.put, "patch": s.patch}
    method = request_mapping.get(type)
    resp = method(endpoint, params=params, json=json, auth=auth)
    logger.info(f"request status {resp.status_code} for endpoint {endpoint}")
    return resp


# todo: maybe create utility classes?
def get_dags(webserver_url, auth):
    dags = urljoin(webserver_url, DAGS)
    resp = _request("get", endpoint=dags, auth=auth)
    return resp.json()


def get_dagruns(webserver_url, dag_id, auth, limit=5) -> dict:
    dagrun_endpoint = urljoin(webserver_url, DAG_RUNS)
    resp = _request(
        type="get",
        endpoint=dagrun_endpoint,
        auth=auth,
        params={"dag_id": dag_id, "limit": limit},
    )
    return resp.json()


def set_dagruns(webserver_url: str, auth, dag_runs: list[dict]) -> dict:
    dagrun_endpoint = urljoin(webserver_url, DAG_RUNS)
    resp = _request(
        type="post", endpoint=dagrun_endpoint, auth=auth, json={"dag_runs": dag_runs}
    )
    return resp.json()


def get_latest_dagrun_state(webserver_url: str, dag_id: str, auth: str) -> str:
    latest = get_dagruns(webserver_url=webserver_url, dag_id=dag_id, auth=auth, limit=1)
    if latest.status_code != 200:
        raise Exception(
            f"Retriveing latest dagrun failed with status: {latest.status_code} {latest.text}"
        )

    return latest[0]["state"]


# another reason for class to couple dagrun and task instance retrieval limits
def get_task_instances(
    webserver_url: str, dag_id: str, auth: str, limit: int = 5
) -> requests.Response:
    task_instances = urljoin(webserver_url, TASK_INSTANCES)
    resp = _request(
        type="get",
        endpoint=task_instances,
        auth=auth,
        params={"dag_id": dag_id, "limit": limit},
    )
    return resp


def set_dag_state(
    webserver_url: str,
    dag_id: str,
    auth,
    action=Literal["pause", "unpause"],
):
    action_dict = {"pause": True, "unpause": False}
    is_paused = action_dict[action]
    payload = {"dag_id": dag_id, "is_paused": is_paused}
    dag_endpoint = urljoin(webserver_url, DAGS)
    return _request(type="patch", endpoint=dag_endpoint, auth=auth, json=payload)


def load_dagruns_to_target(source_url, target_url, dag_id, source_auth, target_auth):
    state = get_latest_dagrun_state(webserver_url=source_url, dag_id=dag_id)
    if state not in (DagRunState.FAILED, DagRunState.SUCCESS):
        logger.info(
            f"Latest dagrun for {dag_id} is not not in state {(DagRunState.FAILED, DagRunState.SUCCESS)}. Skipping migration."
        )
    else:
        set_dag_state(
            webserver_url=source_url, dag_id=dag_id, action="pause", auth=source_auth
        )
        dagruns = get_dagruns(webserver_url=source_url, dag_id=dag_id, auth=source_auth)
        set_dagruns(
            webserver_url=target_url, dag_runs=dagruns["dag_runs"], auth=target_auth
        )


class StarshipOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        conf = context["conf"].as_dict()
        all_dags = get_dags(webserver_url=conf["source_url"], auth=conf["source_auth"])
        for dag in all_dags:
            load_dagruns_to_target(
                dag_id=dag["dag_id"],
                source_url=conf["source_url"],
                source_auth=conf["source_auth"],
                target_url=conf["target_url"],
                target_auth=conf["target_auth"],
            )
