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

from typing import Optional
from datetime import datetime
import time
import os
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class SystemsManagerHook(AwsBaseHook):
    """
    Interact with Amazon Systems Manager Service.
    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.
    .. see also::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(client_type="ssm", *args, **kwargs)
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

    def get_secret(self, secret_name: str) -> Optional[str]:
        """
        Get secret value from Parameter Store.
        :param secret_name: Name of the secret
        """
        try:
            response = self.client.get_parameter(Name=secret_name, WithDecryption=True)
            return response["Parameter"]["Value"]
        except self.client.exceptions.ParameterNotFound:
            self.log.info("Parameter %s not found.", secret_name)
            return None

    def store_secret(
        self,
        secret_name: str,
        secret_value: str,
        overwrite: bool = False,
    ):
        return self.client.put_parameter(
            Name=secret_name,
            Value=secret_value,
            Type="SecureString",
            Overwrite=overwrite,
        )
