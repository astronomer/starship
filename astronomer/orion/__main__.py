# -*- coding: utf-8 -*-
import logging
import os
import shelve
import sys
from pathlib import Path

import click as click
import sh
from click import echo
from click.exceptions import ClickException

from orion import airflow_to_runtime_version, is_file_empty
from orion import version as orion_version
from orion.astro import DEPLOYMENT_FILE_TEMPLATE, DEPLOYMENT_FILE_TEMPLATE_DEFAULTS
from orion.aws import AWS

log = logging.getLogger(__name__)
log.setLevel(
    os.getenv("LOG_LEVEL", logging.INFO),
)
log.addHandler(logging.StreamHandler())

MAIN_ECHO_COLOR = "blue"
SKIP_ECHO_COLOR = "grey"
YES = False


# noinspection PyUnusedLocal
def version(ctx, self, value):
    if not value or ctx.resilient_parsing:
        return
    echo(f"Orion, version {orion_version}")
    ctx.exit()


d = {"show_default": True, "show_envvar": True}
sh_args = {"_in": sys.stdin, "_out": sys.stdout, "_err": sys.stderr, "_tee": True}


def main_echo(msg) -> None:
    click.echo(click.style(msg, fg=MAIN_ECHO_COLOR, bold=True))


def skip_echo(msg) -> None:
    click.echo(click.style(msg, fg=SKIP_ECHO_COLOR))


def confirm_or_exit(msg: str):
    if not YES:
        if not click.confirm(
            click.style(msg + " Continue?", fg=MAIN_ECHO_COLOR, bold=True), default=True
        ):
            raise ClickException("Exiting!")
    else:
        main_echo(msg)


def confirm_or_skip(msg: str) -> bool:
    if not YES:
        if not click.confirm(
            click.style(msg + " Continue?", fg=MAIN_ECHO_COLOR, bold=True), default=True
        ):
            skip_echo("Skipping...")
            return True
    else:
        main_echo(msg)
    return False


def check_workspace(state, workspace):
    if state.get("workspace_initialized") == workspace:
        skip_echo(
            "Astronomer Cloud Workspace already checked for existence, skipping..."
        )
    else:
        confirm_or_exit(
            f"Checking for existence of Astronomer Workspace '{workspace}'..."
        )
        if workspace not in sh.astro("workspace", "list", **sh_args):
            raise ClickException(
                f"Workspace '{workspace}' doesn't exist, or Astro CLI is incorrectly setup! "
                "Create workspace at https://cloud.astronomer.io, or use `astro login`"
            )
        else:
            state["workspace_initialized"] = workspace


def find_environments(state, cloud_client, cloud):
    instances = state.setdefault(
        "instances", {k: {} for k in cloud_client.get_instance_names()}
    )
    if not len(instances):
        raise ClickException(
            f"No {cloud} Environments Found in {cloud_client.region}, exiting!"
        )
    confirm_or_exit(f"Found {len(instances)} environments...")
    return instances


def ensure_project_parent_directory(directory):
    if not directory.exists():
        confirm_or_exit(f"Directory {directory.resolve()} not found, creating...")
        directory.mkdir()


def astro_deploy(deployment):
    # TODO - check if deployment exists
    if not confirm_or_skip(f"Deploying project to Astronomer..."):
        sh.astro("deploy", f"--deployment-name={deployment.name}", **sh_args)


def astro_deploy_create(deployment_config_file, deployment_state, counter_prefix):
    # TODO - check if deployment exists
    if deployment_state.get("deployment_initialized"):
        if not confirm_or_skip(
            f"Creating deployment from file {deployment_config_file.resolve()}..."
        ):
            sh.astro(
                "deployment",
                "create",
                f"--deployment-file={deployment_config_file.resolve()}",
                **sh_args,
            )
            deployment_state["deployment_initialized"] = True
    else:
        skip_echo(f"{counter_prefix} deployment already created, skipping...")


def create_deployment_config_yaml(
    cloud_client,
    cluster,
    deployment,
    deployment_path,
    workspace,
    deployment_state,
    counter_prefix,
):
    deployment_config_file: Path = deployment_path / "config.yaml"
    if deployment_config_file.exists() or deployment_state.get(
        "deployment_config_yaml_initialized"
    ):
        alert_email = click.prompt(
            click.style(
                f"Alert Email for Deployment {deployment.name}",
                fg=MAIN_ECHO_COLOR,
                bold=True,
            ),
            type=str,
        )
        with click.open_file(str(deployment_config_file.resolve()), "w") as f:
            f.write(
                DEPLOYMENT_FILE_TEMPLATE.format(
                    **(
                        DEPLOYMENT_FILE_TEMPLATE_DEFAULTS
                        | cloud_client.get_deployment_template_dict(
                            deployment, cluster, workspace, alert_email
                        )
                    )
                )
            )
        deployment_state["deployment_config_yaml_initialized"] = True
    else:
        skip_echo(f"{counter_prefix} deployment config.yaml exists, skipping...")

    return deployment_config_file


def pytest_project(counter_prefix, deployment_path, deployment_state, pytest):
    if not deployment_state.get("pytest", False) and pytest:
        main_echo(f"{counter_prefix} running `astro dev pytest`...")
        sh.astro("dev", "pytest", _cwd=deployment_path.resolve(), **sh_args)
        deployment_state["pytest"] = True
    else:
        skip_echo(f"{counter_prefix} skipping `astro dev pytest`...")


def parse_project(counter_prefix, deployment_path, deployment_state, parse):
    if not deployment_state.get("parse", False) and parse:
        main_echo(f"{counter_prefix} running `astro dev parse`...")
        sh.astro("dev", "parse", _cwd=deployment_path.resolve(), **sh_args)
        deployment_state["parse"] = True
    else:
        skip_echo(f"{counter_prefix} skipping `astro dev parse`...")


def fill_dags(
    cloud_client, counter_prefix, deployment, deployment_path, deployment_state
):
    dags_path = deployment_path / "dags"
    log.debug("Removing example dags from /dags")
    (dags_path / "example_dag_advanced.py").unlink(missing_ok=True)
    (dags_path / "example_dag_basic.py").unlink(missing_ok=True)
    (dags_path / ".airflowignore").unlink(missing_ok=True)
    if deployment_state.get("dags_initialized", False) or any(dags_path.iterdir()):
        # state[project]["dags_initialized"] is true, or /dags has stuff in it
        skip_echo(f"{counter_prefix} DAGs folder is not empty, skipping...")
    else:
        confirm_or_exit(
            f"{counter_prefix} Saving DAGs folder "
            f"from {deployment.dag_path} to {dags_path}..."
        )
        cloud_client.save_dags(deployment, dags_path)
        deployment_state["dags_initialized"] = True


def fill_plugins(
    cloud_client, counter_prefix, deployment, deployment_path, deployment_state
):
    # TODO
    plugins = deployment_path / "plugins"
    if (
        deployment.plugin_path is None
        or deployment_state.get("plugins_initialized", False)
        or (plugins.exists() and any(plugins.iterdir()))
    ):
        # state[project]["plugins_initialized"] is true, or /plugins has stuff in it
        skip_echo(f"{counter_prefix} Skipping Plugins...")
    else:
        confirm_or_exit(
            f"{counter_prefix} Saving /plugins "
            f"from {deployment.plugin_path} to {plugins}..."
        )
        cloud_client.save_plugins(deployment, plugins)
        deployment_state["plugins_initialized"] = True


def fill_packages_txt(
    cloud_client, counter_prefix, deployment, deployment_path, deployment_state
):
    packages_txt = deployment_path / "packages.txt"
    if deployment_state.get("packages_initialized", False) or (
        packages_txt.exists() and not is_file_empty(packages_txt)
    ):
        # state[project]["packages_initialized"] is true, or packages.txt has stuff in it
        skip_echo(f"{counter_prefix} Packages.txt is not empty, skipping...")
    else:
        confirm_or_exit(
            f"{counter_prefix} Saving System dependencies "
            f"from ??? to {packages_txt}..."
        )
        cloud_client.save_packages(deployment, packages_txt)
        deployment_state["packages_initialized"] = True


def fill_requirements_txt(
    cloud_client, counter_prefix, deployment, deployment_path, deployment_state
):
    requirements_txt = deployment_path / "requirements.txt"
    if deployment_state.get("requirements_initialized", False) or (
        requirements_txt.exists() and not is_file_empty(requirements_txt)
    ):
        # state[project]["requirements_initialized"] is true, or requirements.txt has stuff in it
        skip_echo(f"{counter_prefix} Requirements.txt is not empty, skipping...")
    else:
        confirm_or_exit(
            f"{counter_prefix} Saving Python dependencies "
            f"from {deployment.requirements_path} to {requirements_txt}..."
        )
        cloud_client.save_requirements(deployment, requirements_txt)
        deployment_state["requirements_initialized"] = True


def git_init(counter_prefix, deployment_path, deployment_state):
    if (
        deployment_state.get("git_initialized", False)
        or (deployment_path / ".git").exists()
    ):
        # state[project]["git_initialized"] is true, or we can find .git
        skip_echo(f"{counter_prefix} Git already initialized, skipping...")
    else:
        confirm_or_exit(
            f"{counter_prefix} Initializing git with `git init` at {deployment_path}..."
        )
        sh.git("init", _cwd=deployment_path.resolve(), **sh_args)
        deployment_state["git_initialized"] = True


def astro_dev_init(
    counter_prefix, deployment, deployment_path, deployment_state, instance
):
    if (
        deployment_state.get("project_initialized", False)
        or (deployment_path / ".astro").exists()
    ):
        # if state[project]["project_initialized"] is true, or we can find .astro:
        skip_echo(
            f"{counter_prefix} {instance} Astronomer project already initialized, skipping..."
        )
    else:
        confirm_or_exit(
            f"{counter_prefix} Initializing Astronomer project with `astro dev init` at {deployment_path}..."
        )
        if runtime_version := airflow_to_runtime_version.get(deployment.version):
            args = [f"--runtime-version={runtime_version}"]
        else:
            args = [f"--airflow-version={deployment.version}"]
        sh.astro("dev", "init", *args, _cwd=deployment_path.resolve(), **sh_args)
    deployment_state["project_initialized"] = True


def make_project_directory(counter_prefix, deployment_state, directory, instance):
    deployment_path = deployment_state.setdefault(
        "path", (directory / instance).resolve()
    )
    if deployment_state.get("folder_created", False) or deployment_path.exists():
        # state[project]["folder_created"] is true or folder already exists
        skip_echo(f"{counter_prefix} {instance} folder already created, skipping...")
    else:
        confirm_or_exit(
            f"{counter_prefix} Creating new project folder for {instance}..."
        )
        deployment_path.mkdir(exist_ok=True)
    deployment_state["folder_created"] = True
    return deployment_path


def fetch_deployment_info(cloud_client, counter_prefix, deployment_state, instance):
    if "deployment" in deployment_state:
        skip_echo(
            f"{counter_prefix} Information for {instance} already fetched, skipping..."
        )
        deployment = deployment_state["deployment"]
    else:
        main_echo(f"{counter_prefix} Fetching {instance} information...")
        deployment = cloud_client.deployment_from_instance(instance)
        log.debug(f"Deployment: {deployment}")
        deployment_state["deployment"] = deployment
    return deployment


@click.command()
@click.option(
    "--cloud",
    default="AWS",
    type=click.Choice(
        [
            "AWS",
            # 'GCP',
            # 'AZ'
        ],
        case_sensitive=False,
    ),
    prompt="What is your Cloud Provider?",
    help="Cloud Provider for Source Airflow",
    show_envvar=True,
)
@click.option(
    "--workspace",
    type=str,
    prompt="Name of the Astronomer Workspace to create or use?",
    help="Astronomer Cloud Workspace Name",
    show_envvar=True,
)
@click.option(
    "--cluster",
    type=str,
    prompt="Name of the Astronomer Cluster to use?",
    help="Astronomer Cloud Cluster Name",
    show_envvar=True,
)
@click.option(
    "--region",
    type=str,
    default=lambda: os.environ.get("AWS_DEFAULT_REGION", None),
    help="Cloud Provider Region for Source Airflow",
    show_envvar=True,
)
@click.option(
    "--directory",
    show_envvar=True,
    default=os.getcwd(),
    type=click.Path(
        resolve_path=True, dir_okay=True, file_okay=False, readable=True, writable=True
    ),
)
@click.option(
    "--pytest/--no-pytest",
    show_envvar=True,
    is_flag=True,
    default=True,
    help="Run `astro dev pytest` locally, to determine if migration was successful",
)
@click.option(
    "--parse/--no-parse",
    show_envvar=True,
    is_flag=True,
    default=True,
    help="Run `astro dev parse` locally, to determine if migration was successful",
)
@click.option(
    "--yes",
    show_envvar=True,
    is_flag=True,
    default=False,
    help="Skip confirmation prompts",
)
# @click.option('--dry-run', show_envvar=True, is_flag=True, default=False, help="Do not actually deploy or create")
@click.version_option()
def migrate(cloud, workspace, cluster, region, directory, pytest, parse, yes):
    global YES
    YES = yes
    directory = Path(directory)
    cloud_client: AWS = {
        "AWS": AWS,
        # 'GCP': GCP,
        # 'AZ': AZ
    }[cloud](region=region)

    # LOAD STATE
    with shelve.open(".orionstate", writeback=True) as state:
        # noinspection PyTypeChecker
        log.debug(f"State: {dict(state)}")

        check_workspace(state, workspace)
        instances = find_environments(state, cloud_client, cloud)
        ensure_project_parent_directory(directory)
        confirm_or_exit(
            f"Creating {len(instances)} Astronomer Deployment Project(s)"
            f" in directory: [{directory.resolve()}]..."
        )
        for i, (instance, deployment_state) in enumerate(instances.items()):
            counter_prefix = f"[{i + 1}/{len(instances)}]"
            deployment = fetch_deployment_info(
                cloud_client, counter_prefix, deployment_state, instance
            )
            deployment_path = make_project_directory(
                counter_prefix, deployment_state, directory, instance
            )

            astro_dev_init(
                counter_prefix, deployment, deployment_path, deployment_state, instance
            )
            git_init(counter_prefix, deployment_path, deployment_state)
            fill_requirements_txt(
                cloud_client,
                counter_prefix,
                deployment,
                deployment_path,
                deployment_state,
            )
            if False:
                fill_packages_txt(
                    cloud_client,
                    counter_prefix,
                    deployment,
                    deployment_path,
                    deployment_state,
                )

            fill_plugins(
                cloud_client,
                counter_prefix,
                deployment,
                deployment_path,
                deployment_state,
            )
            fill_dags(
                cloud_client,
                counter_prefix,
                deployment,
                deployment_path,
                deployment_state,
            )
            parse_project(counter_prefix, deployment_path, deployment_state, parse)
            pytest_project(counter_prefix, deployment_path, deployment_state, pytest)
            deployment_config_file = create_deployment_config_yaml(
                cloud_client,
                cluster,
                deployment,
                deployment_path,
                workspace,
                deployment_state,
                counter_prefix,
            )
            astro_deploy_create(
                deployment_config_file, deployment_state, counter_prefix
            )
            astro_deploy(deployment)

            # Starship on Source
            if False:
                cloud_client.add_starship_to_source(deployment)

            # Trust Policies
            print("Still to do - Add Trust Policy to MWAA Role")

            # CICD
            print("Still to do - Add CICD")

            # Secrets Backend
            print("Still to do - Add Secrets Backend")

            # Airflow Configuration
            print("Still to do - Get Airflow configuration")

    logging.debug("Ran successfully, removing .orionstate")
    Path(".orionstate").unlink()


if __name__ == "__main__":
    migrate(auto_envvar_prefix="ORION")
