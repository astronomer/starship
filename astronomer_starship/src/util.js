import axios from 'axios';

/**
 * Returns the PAT URL for a given Airflow URL
 * for Astro that's https://cloud.astronomer.io/token
 * for Software (like https://deployments.basedomain.com/airflow/...) it's https://basedomain.com/token
 * @param targetUrl
 * @returns {string}
 *
 * > tokenUrlFromAirflowUrl('https://cloud.astronomer.io/...')
 * 'https://cloud.astronomer.io/token'
 */
export function tokenUrlFromAirflowUrl(targetUrl) {
  // Software
  if (!targetUrl.includes('astronomer.run')) {
    const urlBody = targetUrl.split('://')[1];
    if (urlBody) {
      const url = urlBody.split('/', 1)[0] || urlBody;
      const basedomain = url.split('deployments.', 2)[1] || url;
      return `https://${basedomain}/token`;
    }
  }
  // Astro
  return 'https://cloud.astronomer.io/token';
}

/**
 * Returns the target URL from the URL parts for Astro or Software
 * @param urlOrgPart - the org part for astro, or basedomain for software
 * @param urlDeploymentPart - the deployment hash for astro or space-name-1234 for software
 * @param isAstro - bool for whether it's astro
 * @returns {string} - url
 */
export function getTargetUrlFromParts(urlOrgPart, urlDeploymentPart, isAstro) {
  return isAstro
    ? `https://${urlOrgPart}.astronomer.run/${urlDeploymentPart}`
    : `https://deployments.${urlOrgPart}/${urlDeploymentPart}/airflow`;
}

/**
 * Parses an Airflow webserver URL and extracts the relevant parts.
 * Supports both Astro and Software URL formats.
 *
 * Astro format: https://{org}.astronomer.run/{deployment}[/home]
 * Software format: https://deployments.{basedomain}/{release-name}/airflow[/home]
 *
 * @param {string} url - The full Airflow webserver URL
 * @returns {{ targetUrl: string, urlOrgPart: string, urlDeploymentPart: string,
 *             isAstro: boolean, isValid: boolean }}
 */
export function parseAirflowUrl(url) {
  const result = {
    targetUrl: '',
    urlOrgPart: '',
    urlDeploymentPart: '',
    isAstro: true,
    isValid: false,
  };

  if (!url || typeof url !== 'string') {
    return result;
  }

  // Clean and normalize URL
  let cleanUrl = url.trim();

  // Add https:// if no protocol specified
  if (!cleanUrl.startsWith('http://') && !cleanUrl.startsWith('https://')) {
    cleanUrl = `https://${cleanUrl}`;
  }

  try {
    const parsed = new URL(cleanUrl);
    const { hostname, pathname } = parsed;

    // Check if it's Astro (*.astronomer.run)
    if (hostname.endsWith('.astronomer.run')) {
      result.isAstro = true;
      // Extract org part (everything before .astronomer.run)
      result.urlOrgPart = hostname.replace('.astronomer.run', '');

      // Extract deployment part from pathname (first segment, ignoring /home)
      const pathParts = pathname.split('/').filter(Boolean);
      // Remove 'home' if it's the last segment
      if (pathParts[pathParts.length - 1] === 'home') {
        pathParts.pop();
      }
      result.urlDeploymentPart = pathParts[0] || '';

      // Build clean target URL
      if (result.urlOrgPart && result.urlDeploymentPart) {
        result.targetUrl = `https://${result.urlOrgPart}.astronomer.run/${result.urlDeploymentPart}`;
        result.isValid = true;
      }
    } else if (hostname.startsWith('deployments.')) {
      // Software format
      result.isAstro = false;
      // Extract basedomain (everything after deployments.)
      result.urlOrgPart = hostname.replace('deployments.', '');

      // Extract release name from pathname
      // pathname is like /release-name-1234/airflow/home
      const pathParts = pathname.split('/').filter(Boolean);
      // First part should be the release name
      result.urlDeploymentPart = pathParts[0] || '';

      // Build clean target URL
      if (result.urlOrgPart && result.urlDeploymentPart) {
        result.targetUrl = `https://deployments.${result.urlOrgPart}/${result.urlDeploymentPart}/airflow`;
        result.isValid = true;
      }
    }
  } catch {
    // Invalid URL
    return result;
  }

  return result;
}

/**
 * Returns the local URL for a given route by splitting at 'starship
 * @param route
 @returns {string}
 */
export function localRoute(route) {
  const localUrl = window.location.href.split('/starship', 1)[0];
  return localUrl + route;
}

/**
 * Returns the remote URL for a given route by combining the target URL and route
 * @param targetUrl
 * @param route
 @returns {string}
 */
export function remoteRoute(targetUrl, route) {
  return targetUrl + route;
}

/**
 * Returns the local proxy URL for a given URL (to avoid CORS issues)
 * @param url
 * @returns {string}
 */
export function proxyUrl(url) {
  return localRoute(`/starship/proxy?url=${encodeURIComponent(url)}`);
}
/**
 * Returns the headers for the proxy (to avoid CORS issues)
 * @param token
 * @returns {{STARSHIP_PROXY_TOKEN}}
 */
export function proxyHeaders(token) {
  return {
    'Starship-Proxy-Token': token,
  };
}

/**
 * Fetches data from both the local and remote endpoints
 * @param localRouteUrl
 * @param remoteRouteUrl
 * @param token
 * @param loadingDispatch - a dispatch route to call to set the loading variable
 * @param dataDispatch - dispatch route to call to set the data variables
 * @param errorDispatch - dispatch route to call to set the error variable
 */
export async function fetchData(
  localRouteUrl,
  remoteRouteUrl,
  token,
  loadingDispatch,
  dataDispatch,
  errorDispatch,
) {
  if (loadingDispatch) {
    loadingDispatch();
  }

  try {
    const [localRes, remoteRes] = await Promise.all([
      axios.get(localRouteUrl),
      axios.get(proxyUrl(remoteRouteUrl), { headers: proxyHeaders(token) }),
    ]);

    if (
      localRes.status === 200
      && localRes.headers['content-type'] === 'application/json'
      && remoteRes.status === 200
      && remoteRes.headers['content-type'] === 'application/json'
    ) {
      dataDispatch(localRes, remoteRes);
    } else {
      errorDispatch(new Error('Invalid response: expected JSON content-type'));
    }
  } catch (err) {
    errorDispatch(err);
  }
}

export function objectWithoutKey(object, key) {
  const { [key]: _, ...otherKeys } = object;
  return otherKeys;
}

/**
 * Constructs and returns the URL for the Astro Deployment Environment Variable API route
 *
 * @param {string} organizationId
 * @param {string} deploymentId
 * @returns {string} - The URL for the Astro Environment Variable service.
 */
export function getAstroEnvVarRoute(organizationId, deploymentId) {
  return `https://api.astronomer.io/platform/v1beta1/organizations/${organizationId}/deployments/${deploymentId}`;
}

/**
 * Constructs and returns the URL for a Houston API
 *
 * @param {string} basedomain
 * @returns {string} - The URL for the Houston service.
 */
export function getHoustonRoute(basedomain) {
  return `https://houston.${basedomain}/v1/`;
}

/**
 * Returns the DAG view URL based on Airflow version
 * Airflow 2.x: /dags/{dag_id}/grid
 * Airflow 3.x: /dags/{dag_id}
 *
 * @param {string} dagId - The DAG ID
 * @param {string} airflowVersion - The Airflow version string (e.g., "2.8.1", "3.0.0")
 * @returns {string} - The path to the DAG view
 */
export function getDagViewPath(dagId, airflowVersion) {
  const majorVersion = parseInt(airflowVersion?.split('.')[0] || '2', 10);
  if (majorVersion >= 3) {
    return `/dags/${dagId}`;
  }
  // Airflow 2.x uses grid view
  return `/dags/${dagId}/grid`;
}
