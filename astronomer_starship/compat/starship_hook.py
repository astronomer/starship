from typing import Literal
from logging import getLogger
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin
from airflow.utils.state import DagRunState
from airflow.hooks.base import BaseHook


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
    auth=None,
    json=None,
    params=None,
    headers=None,
    retries=3,
    backoff_factor=2,
):
    s = session_with_retry(retries=retries, backoff_factor=backoff_factor)
    request_mapping = {"get": s.get, "post": s.post, "put": s.put, "patch": s.patch}
    method = request_mapping.get(type)
    resp = method(endpoint, params=params, json=json, auth=auth, headers=headers)
    logger.info(f"request status {resp.status_code} for endpoint {endpoint}")
    return resp


class StarshipAPIHook(BaseHook):

    DAG_RUNS = "/api/starship/dag_runs"
    TASK_INSTANCES = "/api/starship/task_instances"
    DAGS = "/api/starship/dags"

    def __init__(
        self,
        webserver_url,
        auth=None,
        headers=None,
        logger_name: str | None = None,
    ):
        super().__init__(logger_name)
        self.webserver_url = webserver_url
        self.auth = auth
        self.headers = headers

    # todo: maybe create utility classes?
    def get_dags(self):
        dags = urljoin(self.webserver_url, StarshipAPIHook.DAGS)
        resp = _request("get", endpoint=dags, auth=self.auth, headers=self.headers)
        return resp.json()

    def get_dagruns(self, dag_id, limit=5) -> dict:
        dagrun_endpoint = urljoin(self.webserver_url, StarshipAPIHook.DAG_RUNS)
        resp = _request(
            type="get",
            endpoint=dagrun_endpoint,
            auth=self.auth,
            headers=self.headers,
            params={"dag_id": dag_id, "limit": limit},
        )
        return resp.json()

    def set_dagruns(
        self,
        dag_runs: list[dict],
    ) -> dict:
        dagrun_endpoint = urljoin(self.webserver_url, StarshipAPIHook.DAG_RUNS)
        resp = _request(
            type="post",
            endpoint=dagrun_endpoint,
            auth=self.auth,
            headers=self.headers,
            json={"dag_runs": dag_runs},
        )
        return resp.json()

    def get_latest_dagrun_state(self, dag_id) -> str:
        latest = self.get_dagruns(
            webserver_url=self.webserver_url,
            dag_id=dag_id,
            auth=self.auth,
            headers=self.headers,
            limit=1,
        )
        if latest.status_code != 200:
            raise Exception(
                f"Retriveing latest dagrun failed with status: {latest.status_code} {latest.text}"
            )

        return latest[0]["state"]

    # another reason for class to couple dagrun and task instance retrieval limits
    def get_task_instances(
        self,
        dag_id: str,
        limit: int = 5,
    ) -> dict:
        task_instances = urljoin(self.webserver_url, StarshipAPIHook.TASK_INSTANCES)
        resp = _request(
            type="get",
            endpoint=task_instances,
            auth=self.auth,
            headers=self.headers,
            params={"dag_id": dag_id, "limit": limit},
        )
        return resp.json()

    def set_task_instances(self, task_instances: list[dict]):
        task_instance_endpoint = urljoin(
            self.webserver_url, StarshipAPIHook.TASK_INSTANCES
        )
        resp = _request(
            type="post",
            endpoint=task_instance_endpoint,
            auth=self.auth,
            headers=self.headers,
            json={"task_instances": task_instances},
        )
        return resp.json()

    def set_dag_state(
        self,
        dag_id: str,
        action=Literal["pause", "unpause"],
    ):
        action_dict = {"pause": True, "unpause": False}
        is_paused = action_dict[action]
        payload = {"dag_id": dag_id, "is_paused": is_paused}
        dag_endpoint = urljoin(self.webserver_url, StarshipAPIHook.DAGS)
        return _request(
            type="patch",
            endpoint=dag_endpoint,
            auth=self.auth,
            headers=self.headers,
            json=payload,
        )


class StarshipDagRunMigrationHook(BaseHook):

    def __init__(
        self,
        source_webserver_url: str,
        target_webserver_url: str,
        source_auth: tuple = None,
        target_auth: tuple = None,
        source_headers: dict = None,
        target_headers: dict = None,
        logger_name: str | None = None,
    ):
        super().__init__(logger_name)

        self.source_api_hook = StarshipAPIHook(
            webserver_url=source_webserver_url, auth=source_auth, headers=source_headers
        )
        self.target_api_hook = StarshipAPIHook(
            webserver_url=target_webserver_url, auth=target_auth, headers=target_headers
        )

    def load_dagruns_to_target(
        self,
        dag_ids: list[str] = None,
    ):
        if not dag_ids:
            dag_ids = self.source_api_hook.get_dags()

        for dag_id in dag_ids:
            state = self.source_api_hook.get_latest_dagrun_state(dag_id=dag_id)
            if state not in (DagRunState.FAILED, DagRunState.SUCCESS):
                logger.info(
                    f"Latest dagrun for {dag_id} is not not in state {(DagRunState.FAILED, DagRunState.SUCCESS)}. Skipping migration."
                )
            else:
                self.source_api_hook.set_dag_state(
                    dag_id=dag_id,
                    action="pause",
                )
                self.get_and_set_dagruns(dag_id)
                self.get_and_set_task_instances(dag_id)

    def get_and_set_dagruns(self, dag_id):
        dag_runs = self.source_api_hook.get_dagruns(
            dag_id=dag_id,
        )
        self.target_api_hook.set_dagruns(dag_runs=dag_runs["dag_runs"])

    def get_and_set_task_instances(self, dag_id):
        task_instances = self.source_api_hook.get_task_instances(dag_id=dag_id)
        self.target_api_hook.set_task_instances(task_instances=task_instances)
