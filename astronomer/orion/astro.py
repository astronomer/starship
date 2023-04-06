# -*- coding: utf-8 -*-

DEPLOYMENT_FILE_TEMPLATE_DEFAULTS = {
    "dag_deploy_enabled": True,
    "scheduler_au": 5,
    "num_schedulers": 1,
    "min_workers": 1,
    "max_workers": 10,
    "worker_concurrency": 16,
}

DEPLOYMENT_FILE_TEMPLATE = """
deployment:
    environment_variables: []
    configuration:
        name: "{name}"
        description: "Imported using Orion from {name} instance"
        runtime_version: {runtime_version}
        dag_deploy_enabled: {dag_deploy_enabled}
        scheduler_au: {scheduler_au}
        scheduler_count: {num_schedulers}
        cluster_name: "{cluster_name}"
        workspace_name: "{workspace_name}"
    worker_queues:
        - name: default
          max_worker_count: {max_workers}
          min_worker_count: {min_workers}
          worker_concurrency: {worker_concurrency}
          worker_type: "{worker_type}"
    alert_emails:
        - "{alert_email}"
"""
