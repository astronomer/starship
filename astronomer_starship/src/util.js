import axios from 'axios';

/**
 * Returns the PAT URL for a given Airflow URL
 * for Astro that's https://cloud.astronomer.io/token
 * for Software (like https://baseurl.domain.com/airflow/...) it's https://baseurl.domain.com/token
 * @param targetUrl
 * @returns {string}
 *
 * > tokenUrlFromAirflowUrl('https://cloud.astronomer.io/...')
 * 'https://cloud.astronomer.io/token'
 */
export function tokenUrlFromAirflowUrl(targetUrl) {
  if (!targetUrl.includes('astronomer.run')) {
    const urlBody = targetUrl.split('://')[1];
    if (urlBody) {
      const url = urlBody.split('/', 1)[0] || urlBody;
      return `https://${url}/token`;
    }
  }
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
    : `https://${urlOrgPart}/${urlDeploymentPart}/airflow`;
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
export function fetchData(
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
  axios
    .get(localRouteUrl)
    .then((res) => {
      axios
        .get(proxyUrl(remoteRouteUrl), { headers: proxyHeaders(token) })
        .then((rRes) => dataDispatch(res, rRes)) // , dispatch))
        .catch((err) => errorDispatch(err)); // , dispatch));
    })
    .catch((err) => errorDispatch(err)); // , dispatch));
}

export function objectWithoutKey(object, key) {
  const { [key]: _, ...otherKeys } = object;
  return otherKeys;
}
