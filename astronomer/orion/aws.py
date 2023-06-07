import os
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional
from zipfile import ZipFile

import boto3
import click
from botocore.exceptions import NoRegionError
from click import ClickException
from mypy_boto3_mwaa.client import MWAAClient
from mypy_boto3_s3 import S3ServiceResource

from orion import CloudProvider, Deployment, airflow_to_runtime_version


class AWS(CloudProvider):
    def __init__(self, region: Optional[str] = None):
        try:
            kwargs = {"region_name": region} if region else {}
            session = boto3.session.Session(**kwargs)
        except NoRegionError:
            region = region or click.prompt(
                text="What region are your Airflow instances in your Cloud Provider?",
                default="us-east-1",
                type=str,
            )
            session = boto3.session.Session(region_name=region)

        self.session = session
        self.region = session.region_name
        self.client: MWAAClient = session.client(service_name="mwaa")
        self.s3: S3ServiceResource = session.resource("s3")

    def get_instance_names(self) -> Optional[List[str]]:
        return self.client.list_environments()["Environments"]

    def deployment_from_instance(self, name: str) -> Deployment:
        instance = self.client.get_environment(Name=name)["Environment"]
        bucket = instance["SourceBucketArn"].replace("arn:aws:s3:::", "")
        return Deployment(
            name=instance["Name"],
            version=instance["AirflowVersion"],
            size=instance["EnvironmentClass"],  # mw1.small, etc
            dag_path=f"s3://{bucket}/{instance['DagS3Path']}",
            requirements_path=f"s3://{bucket}/{instance['RequirementsS3Path']}",
            plugin_path=f"s3://{bucket}/{instance.get('PluginsS3Path')}"
            if instance.get("PluginsS3Path")
            else None,
            execution_role=instance["ExecutionRoleArn"],  # other role 'ServiceRoleArn'
            min_workers=instance["MinWorkers"],
            max_workers=instance["MaxWorkers"],
            num_schedulers=instance["Schedulers"],
            tags=instance["Tags"],
            status=instance["Status"],  # e.g. CREATE_FAILED
            configuration=instance["AirflowConfigurationOptions"],
        )

    def save_requirements(self, deployment: Deployment, local_path: str) -> None:
        pathparts = Path(deployment.requirements_path).parts
        bucket = pathparts[1]
        path = "/".join(pathparts[2:])
        self.s3.Object(bucket_name=bucket, key=path).download_file(str(local_path))

        # bucket = "//".join(pathparts[0:2])
        # s3fs = S3FS(bucket_name=bucket)
        # copy_file(s3fs, path, './', str(local_path))

    def save_packages(self, deployment: Deployment, local_path: str) -> None:
        pass
        # copy_fs(deployment., local_path)
        # raise NotImplementedError()

    def save_dags(self, deployment: Deployment, local_path: str) -> None:
        def download_s3_folder(bucket_name, s3_folder, local_dir=None):
            """
            Download the contents of a folder directory
            Args:
                bucket_name: the name of the s3 bucket
                s3_folder: the folder path in the s3 bucket
                local_dir: a relative or absolute directory path in the local file system
            """
            _bucket = self.s3.Bucket(bucket_name)
            for obj in _bucket.objects.filter(Prefix=s3_folder):
                target = (
                    obj.key
                    if local_dir is None
                    else os.path.join(local_dir, os.path.relpath(obj.key, s3_folder))
                )
                if not os.path.exists(os.path.dirname(target)):
                    os.makedirs(os.path.dirname(target))
                if obj.key[-1] == "/":
                    continue
                _bucket.download_file(obj.key, target)

        pathparts = Path(deployment.dag_path).parts
        bucket = pathparts[1]
        path = "/".join(pathparts[2:])
        download_s3_folder(bucket_name=bucket, s3_folder=path, local_dir=local_path)
        # copy_dir(
        #     # src_fs=bucket,
        #     src_path=deployment.dag_path,
        #     # dst_fs="",
        #     dst_path=local_path,
        # )

    def save_plugins(self, deployment: Deployment, local_path: str) -> None:
        pathparts = Path(deployment.plugin_path).parts
        bucket = pathparts[1]
        path = "/".join(pathparts[2:])
        with BytesIO() as filebytes, ZipFile(filebytes) as zipfile:
            self.s3.Object(bucket_name=bucket, key=path).download_fileobj(filebytes)
            zipfile.extractall(local_path)

    def add_starship_to_source(self, deployment: Deployment) -> None:
        # TODO
        self.client.update_environment(
            Name=deployment.name, RequirementsS3Path="", RequirementsS3ObjectVersion=""
        )
        raise NotImplementedError()

    def get_deployment_template_dict(
        self,
        deployment: Deployment,
        cluster_name: str,
        workspace_name: str,
        alert_email: Optional[str],
    ) -> Dict[str, Any]:
        template_dict = {
            "name": deployment.name,
            "cluster_name": cluster_name,
            "workspace_name": workspace_name,
            "worker_type": "e2-standard-4"  # TODO
            # "dag_deploy_enabled": True,
            # "scheduler_au": 5,
            # "num_schedulers": 1,
            # "min_workers": 1,
            # "max_workers": 10,
            # "worker_concurrency": 16,
        }
        if deployment.version in airflow_to_runtime_version:
            template_dict["runtime_version"] = airflow_to_runtime_version[
                deployment.version
            ]
        else:
            raise ClickException(
                f"Airflow Version {deployment.version} unable to be mapped to runtime version, "
                f"available versions: {airflow_to_runtime_version.values()}"
            )

        if alert_email:
            template_dict["alert_email"] = alert_email

        return template_dict
