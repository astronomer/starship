/**
 * Returns the PAT URL for a given Airflow URL
 * for Astro that's https://cloud.astronomer.io/token
 * for Software (like https://baseurl.domain.com/airflow/...) it's https://baseurl.domain.com/token
 * @param targetUrl
 * @returns {string}
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
 * @param isAstro - bool for whether or not it's astro
 * @returns {string} - url
 */
export function getTargetUrlFromParts(urlOrgPart, urlDeploymentPart, isAstro) {
  return isAstro
    ? `https://${urlOrgPart}.astronomer.run/${urlDeploymentPart}/`
    : `https://${urlOrgPart}/${urlDeploymentPart}/airflow/`;
}
