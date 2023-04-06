# -*- coding: utf-8 -*-
"""A tool to easily migrate Airflow Environments from any Cloud Provider to Astronomer!"""
import sys
from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from fs import open_fs
from fs.copy import copy_dir, copy_fs

if sys.version_info >= (3, 8):
    from importlib import metadata as importlib_metadata
else:
    import importlib_metadata


def get_version() -> str:
    try:
        return importlib_metadata.version(__name__)
    except importlib_metadata.PackageNotFoundError:  # pragma: no cover
        return "unknown"


__version__: str = get_version()
version: str = __version__

airflow_to_runtime_version = {
    "2.0.2": "3.0.4",  # Actually Airflow 2.1.1
    "2.2.2": "4.2.9",  # Actually Airflow 2.2.5
    "2.4.3": "6.3.0",  # Airflow 2.4.3
}


def is_file_empty(p: Path) -> bool:
    return p.stat().st_size == 0


@dataclass(frozen=True)
class Deployment:
    name: str
    version: str
    size: str
    dag_path: str = "dags"
    requirements_path: str = "requirements.txt"
    plugin_path: str = "plugins"
    execution_role: str = None
    min_workers: int = -1
    max_workers: int = -1
    num_schedulers: int = -1
    tags: Dict[str, str] = None
    status: str = None
    configuration: Dict[str, str] = None


class CloudProvider:
    region: str

    @abstractmethod
    def get_instance_names(self) -> Optional[List[str]]:
        """Get a list of from an MWAA or GCC Instance names"""

    @abstractmethod
    def deployment_from_instance(self, name: str) -> Deployment:
        """Create a Deployment from an MWAA or GCC Instance"""

    @abstractmethod
    def save_requirements(self, deployment: Deployment, local_path: str) -> None:
        # open_fs(deployment.requirements_path).copy_file(deployment.requirements_path, local_path)
        pass

    @abstractmethod
    def save_packages(self, deployment: Deployment, local_path: str) -> None:
        pass

    @abstractmethod
    def save_plugins(self, deployment: Deployment, local_path: str) -> None:
        pass

    @abstractmethod
    def save_dags(self, deployment: Deployment, local_path: str) -> None:
        pass

    @abstractmethod
    def add_starship_to_source(self, deployment: Deployment) -> None:
        pass

    @abstractmethod
    def get_deployment_template_dict(
        self,
        deployment: Deployment,
        cluster_name: str,
        workspace_name: str,
        alert_email: Optional[str],
    ) -> Dict[str, Any]:
        pass
