const constants = Object.freeze({
  TELESCOPE_ROUTE: '/api/starship/telescope',
  ENV_VAR_ROUTE: '/api/starship/env_vars',
  POOL_ROUTE: '/api/starship/pools',
  CONNECTIONS_ROUTE: '/api/starship/connections',
  VARIABLES_ROUTE: '/api/starship/variables',
  DAGS_ROUTE: '/api/starship/dags',
  DAG_RUNS_ROUTE: '/api/starship/dag_runs',
  TASK_INSTANCE_ROUTE: '/api/starship/task_instances',
  TASK_INSTANCE_HISTORY_ROUTE: '/api/starship/task_instance_history',
  SOURCE_CONNECTION_ROUTE: '/api/starship/source_connection',
  CUTOVER_WAVES_ROUTE: '/api/starship/cutover/waves',
  CUTOVER_PURGE_ALL_ROUTE: '/api/starship/cutover/purge_all',
});
export default constants;

/**
 * Route paths for navigation (without leading slash for router config)
 */
export const ROUTES = Object.freeze({
  SETUP: 'setup',
  SOURCE_SETUP: 'source-setup',
  VARIABLES: 'variables',
  CONNECTIONS: 'connections',
  POOLS: 'pools',
  ENV_VARS: 'env',
  DAGS: 'dags',
  TELESCOPE: 'telescope',
  CUTOVER: 'cutover',
});

/**
 * Source platforms supported by the Cutover Tool.
 * Must stay in sync with SUPPORTED_SOURCE_PLATFORMS in astronomer_starship/common.py.
 */
export const SOURCE_PLATFORMS = Object.freeze([
  {
    id: 'astro',
    label: 'Astro',
    description: 'Astronomer Cloud or Software deployment. Uses a Deployment API token.',
  },
  {
    id: 'gcc',
    label: 'Google Cloud Composer',
    description: 'Composer 2/3. Uses Application Default Credentials (ADC), with optional service-account impersonation.',
  },
  {
    id: 'mwaa',
    label: 'Amazon MWAA',
    description: 'Managed Workflows for Apache Airflow. Uses an IAM role via boto3.',
  },
  {
    id: 'oss',
    label: 'OSS Airflow',
    description: 'Self-hosted Airflow. Uses a bearer token or HTTP Basic auth.',
  },
]);

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
