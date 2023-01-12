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
#

from typing import Any, Dict, Union
import os
from airflow.providers.amazon.aws.hooks.secrets_manager import (
    SecretsManagerHook as BaseSecretsManagerHook,
)


class SecretsManagerHook(BaseSecretsManagerHook):
    """
    Interact with Amazon SecretsManager Service.
    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.
    .. see also::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        DEFAULT_CONNECTIONS_PREFIX = "/airflow/connections"
        DEFAULT_VARIABLES_PREFIX = "/airflow/variables"
        DEFAULT_SECRETS_SEPARATOR = "/"
        DEFAULT_OVERWRITE = "True"
        self.variables_prefix = os.environ.get(
            "VARIABLES_PREFIX", DEFAULT_VARIABLES_PREFIX
        )
        self.connections_prefix = os.environ.get(
            "CONNECTIONS_PREFIX", DEFAULT_CONNECTIONS_PREFIX
        )
        self.separator = os.environ.get("SECRETS_SEPARATOR", DEFAULT_SECRETS_SEPARATOR)
        self.overwrite_existing = os.environ.get(
            "OVERWRITE_EXISTING", DEFAULT_OVERWRITE
        )
        self.client = self.get_conn()

    def store_secret(
        self, secret_name: str, secret_value: Union[str, bytes], overwrite: bool = False
    ):
        kwargs: Dict[str, Any] = {}
        if isinstance(secret_value, str):
            kwargs["SecretString"] = secret_value
        elif isinstance(secret_value, bytes):
            kwargs["SecretBinary"] = secret_value

        if overwrite:
            try:
                return self.client.put_secret_value(SecretId=secret_name, **kwargs)
            except self.client.exceptions.ResourceNotFoundException:
                self.log.info("Resource doesn't exist, try creating")
        return self.client.create_secret(Name=secret_name, **kwargs)
