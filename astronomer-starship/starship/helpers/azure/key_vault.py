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
import os
from typing import Any, Optional

from airflow.compat.functools import cached_property
from airflow.hooks.base import BaseHook

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.identity._constants import EnvironmentVariables
from azure.keyvault.secrets import SecretClient


class AzureKeyVaultHook(BaseHook):
    """
    Retrieves Airflow Connections or Variables from Azure Key Vault secrets.
    For client authentication, the ``DefaultAzureCredential`` from the Azure Python SDK is used as
    credential provider, which supports service principal, managed identity and user credentials
    For example, to specify a service principal with secret you can set the environment variables
    ``AZURE_TENANT_ID``, ``AZURE_CLIENT_ID`` and ``AZURE_CLIENT_SECRET``.
    .. seealso::
        For more details on client authentication refer to the ``DefaultAzureCredential`` Class reference:
        https://docs.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python
    :param vault_url: The URL of an Azure Key Vault to use
    """

    conn_name_attr = "azure_conn_id"
    default_conn_name = "azure_default"
    conn_type = "azure"
    hook_name = "Azure Key Vault"

    def __init__(
        self,
        azure_conn_id: str = default_conn_name,
        separator: str = "-",
        **kwargs,
    ) -> None:
        super().__init__()
        self.conn_id = azure_conn_id
        self.separator = separator
        self.kwargs = kwargs


    @cached_property
    def client(self) -> SecretClient:
        """Create a Azure Key Vault client."""
        return self.get_conn()

    def get_conn(self) -> Any:
        """
        Authenticates the resource using the connection id passed during init.
        :return: the authenticated client.
        """
        conn = self.get_connection(self.conn_id)
        tenant = conn.extra_dejson.get(
            "extra__azure__tenantId"
        ) or conn.extra_dejson.get("tenantId")
        client_id = conn.login
        client_secret = conn.password
        vault_url = conn.host

        azure_creds = {
            EnvironmentVariables.AZURE_TENANT_ID: tenant,
            EnvironmentVariables.AZURE_CLIENT_ID: client_id,
            EnvironmentVariables.AZURE_CLIENT_SECRET: client_secret,
        }
        os.environ.update(azure_creds)
        credential = DefaultAzureCredential()
        return SecretClient(
            vault_url=str(vault_url), credential=credential, **self.kwargs
        )

    def get_secret(self, secret_name: str) -> Optional[str]:
        """
        Get an Azure Key Vault secret value
        :param path_prefix: Prefix for the Path to get Secret
        :param secret_id: Secret Key
        """
        secret_name = secret_name.replace("_", self.separator)
        try:
            secret = self.client.get_secret(name=secret_name)
            return secret.value
        except ResourceNotFoundError as ex:
            self.log.debug("Secret %s not found: %s", secret_name, ex)
            return None

    def store_secret(
        self, secret_name: str, secret_value: str, overwrite: bool = False
    ):
        secret_name = secret_name.replace("_", self.separator)
        if not overwrite:
            secret = self.get_secret(secret_name=secret_name)
            if secret is not None:
                raise ResourceExistsError(f"Secret {secret_name} already exists")
        return self.client.set_secret(name=secret_name, value=secret_value)