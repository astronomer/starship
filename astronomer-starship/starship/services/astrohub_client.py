from typing import Optional


class AstroClient:
    def __init__(
        self, token: Optional[str] = None, url: str = "https://api.astronomer.io/hub/v1"
    ):
        self.token = token
        self.url = url

    def list_deployments(self):
        headers = {"Authorization": f"Bearer {self.token}"}
        from python_graphql_client import GraphqlClient

        client = GraphqlClient(endpoint=self.url, headers=headers)
        query = """
                    {
                     deployments
                     {
                         id,
                         label,
                         releaseName,
                         workspace
                         {
                             id,
                             label
                         },
                         deploymentShortId,
                         deploymentSpec
                         {
                             environmentVariables
                             webserver {
                                 ingressHostname,
                                 url
                             }
                         }
                     }
                    }
                    """

        return client.execute(query).get("data")
