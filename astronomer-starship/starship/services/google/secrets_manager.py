#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Hook for Secrets Manager service"""
from typing import Optional, Sequence, Union

from airflow.providers.google.cloud._internal_client.secret_manager_client import (
    _SecretManagerClient as _BaseSecretManagerClient,
)
from airflow.providers.google.cloud.hooks.secret_manager import (
    SecretsManagerHook as BaseSecretsManagerHook,
)
from airflow.providers.google.cloud.utils.credentials_provider import get_credentials_and_project_id
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

from google.api_core.exceptions import AlreadyExists
import google
import os
from datetime import datetime
import time

class _SecretManagerClient(_BaseSecretManagerClient):
    """
    Retrieves Secrets object from Google Cloud Secrets Manager. This is a common class reused between
    SecretsManager and Secrets Hook that provides the shared authentication and verification mechanisms.
    This class should not be used directly, use SecretsManager or SecretsHook instead
    :param credentials: Credentials used to authenticate to GCP
    """
    def __init__(self, credentials: google.auth.credentials.Credentials,):
        super().__init__(credentials=credentials)

    def create_secret(self, project_id: str, secret_id: str) -> str:
        """
        Create a new secret with the given name. A secret is a logical wrapper
        around a collection of secret versions. Secret versions hold the actual
        secret material.
        """
        # Create the secret.
        secret = self.client.create_secret(
            parent=self.client.project_path(project_id),
            secret_id=secret_id,
            secret={"replication": {"automatic": {}}},
        )

        # Print the new secret name.
        self.log.info(f"Created secret: {secret.name}")

        return secret

    def add_secret_version(self, project_id: str, secret_id: str, payload: str) -> str:
        """
        Add a new secret version to the given secret with the provided payload.
        """
        # Convert the string payload into a bytes. This step can be omitted if you
        # pass in bytes instead of a str for the payload argument.
        payload_bytes = payload.encode("UTF-8")

        # Add the secret version.
        secret_version = self.client.add_secret_version(
            parent=self.client.secret_path(project_id, secret_id),
            payload={
                "data": payload_bytes,
            },
        )

        # Print the new secret version name.
        self.log.info(f"Added secret version: {secret_version.name}")

        return secret_version


class SecretsManagerHook(BaseSecretsManagerHook):
    """
    Hook for the Google Secret Manager API.
    See https://cloud.google.com/secret-manager
    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        print(f"DEBUG: {datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}:: class init")

        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        print(f"DEBUG: {datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}:: super called")
        DEFAULT_CONNECTIONS_PREFIX = "airflow-connections"
        DEFAULT_VARIABLES_PREFIX = "airflow-variables"
        DEFAULT_SECRETS_SEPARATOR = "-"
        DEFAULT_OVERWRITE = "True"
        self.variables_prefix = os.environ.get("VARIABLES_PREFIX", DEFAULT_VARIABLES_PREFIX)
        self.connections_prefix = os.environ.get("CONNECTIONS_PREFIX", DEFAULT_CONNECTIONS_PREFIX)
        self.separator = os.environ.get("SECRETS_SEPARATOR", DEFAULT_SECRETS_SEPARATOR)
        self.overwrite_existing = os.environ.get("OVERWRITE_EXISTING", DEFAULT_OVERWRITE)
        print(f"DEBUG: {datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}:: envs set")
        try:
            self.project_id=get_credentials_and_project_id()[1]
            print(f"DEBUG: {datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}:: project id is{self.project_id}")

        except Exception as e:
            print(f"ERROR: {datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}:: {e}")
        self.client = _SecretManagerClient(credentials=self.get_credentials())
        print(f"DEBUG: {datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}:: client called")


    def get_conn(self) -> _SecretManagerClient:
        """
        Retrieves the connection to Secret Manager.
        :return: Secret Manager client.
        :rtype: airflow.providers.google.cloud._internal_client.secret_manager_client._SecretManagerClient
        """
        return self.client

    @GoogleBaseHook.fallback_to_default_project_id
    def store_secret(
        self,
        secret_name: str,
        secret_value: str,
        overwrite: bool = False,
        project_id: Optional[str] = None,
    ):
        try:
            self.client.create_secret(project_id=project_id, secret_id=secret_name)  # type: ignore
        except AlreadyExists as e:
            if not overwrite:
                self.log.error(
                    f"Secret {secret_name} already exists in project {project_id}"
                )
                raise e
        secret_version = self.client.add_secret_version(
            project_id=project_id, secret_id=secret_name, payload=secret_value  # type: ignore
        )
        return secret_version

    def get_secret(self, secret_name: str) -> Optional[str]:

        # secret_name = secret_name.replace("_", self.separator)
        try:
            secret = self.client.get_secret(secret_id=secret_name,project_id=self.project_id)
            print(f"the secret is {secret}")
            print(f"the secret type is {type(secret)}")
            return secret
        except Exception as ex:
            print("Secret %s not found: %s", secret_name, ex)
            self.log.debug("Secret %s not found: %s", secret_name, ex)
            return None