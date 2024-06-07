const constants = {
  TELESCOPE_ROUTE: '/api/starship/telescope',
  ENV_VAR_ROUTE: '/api/starship/env_vars',
  POOL_ROUTE: '/api/starship/pools',
  CONNECTIONS_ROUTE: '/api/starship/connections',
  VARIABLES_ROUTE: '/api/starship/variables',
  DAGS_ROUTE: '/api/starship/dags',
  DAG_RUNS_ROUTE: '/api/starship/dag_runs',
  TASK_INSTANCE_ROUTE: '/api/starship/task_instances',
};
export default constants;

export const updateDeploymentVariablesMutation = `
mutation UpdateDeploymentVariables(
  $deploymentUuid:Uuid!,
  $releaseName:String!,
  $environmentVariables: [InputEnvironmentVariable!]!
) {
  updateDeploymentVariables(
    deploymentUuid: $deploymentUuid,
    releaseName: $releaseName,
    environmentVariables: $environmentVariables
  ) {
    key
    value
    isSecret
  }
}`;

export const getDeploymentsQuery = `query deploymentVariables($deploymentUuid: Uuid!, $releaseName: String!) {
  deploymentVariables(
    deploymentUuid: $deploymentUuid
    releaseName: $releaseName
  ) {
    key
    value
    isSecret
  }
}`;

export const getWorkspaceDeploymentsQuery = `
query workspaces {
  workspaces {
    id
    deployments {
      id
      releaseName
    }
  }
}`;
