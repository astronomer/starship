import throttle from 'lodash.throttle';
import { useCallback, useEffect, useRef } from 'react';
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
 * Returns the proxy URL for a given URL (to avoid CORS issues)
 * @param url
 * @returns {string}
 */
export function proxyUrl(url) {
  return `proxy?url=${encodeURIComponent(url)}`;
}

/**
 * Returns the headers for the proxy (to avoid CORS issues)
 * @param token
 * @returns {{STARSHIP_PROXY_TOKEN}}
 */
export function proxyHeaders(token) {
  return {
    STARSHIP_PROXY_TOKEN: token,
  };
}

/** TODO - Deprecated
 * From https://stackoverflow.com/a/62017005
 * Usage:
 *   useEffect(useThrottle(() => console.log(value), 1000), [value]);
 * @param cb
 * @param delay
 * @returns {(function(): (*))|*}
 */
export function useThrottle(cb, delay) {
  const options = { leading: true, trailing: false }; // add custom lodash options
  const cbRef = useRef(cb);
  // use mutable ref to make useCallback/throttle not depend on `cb` dep
  useEffect(() => { cbRef.current = cb; });
  return useCallback(
    throttle((...args) => cbRef.current(...args), delay, options),
    [delay],
  );
}

/**
 * Fetches data from both the local and remote endpoints
 * @param localRoute
 * @param remoteRoute
 * @param token
 * @param loadingDispatch - a dispatch route to call to set the loading variable
 * @param dataDispatch - dispatch route to call to set the data variables
 * @param errorDispatch - dispatch route to call to set the error variable
 * @param dispatch - global dispatch
 */
export function fetchData(
  localRoute,
  remoteRoute,
  token,
  loadingDispatch,
  dataDispatch,
  errorDispatch,
) {
  if (loadingDispatch) {
    loadingDispatch();
  }
  axios
    .get(localRoute)
    .then((res) => {
      axios
        .get(proxyUrl(remoteRoute), { headers: proxyHeaders(token) })
        .then((rRes) => dataDispatch(res, rRes)) // , dispatch))
        .catch((err) => errorDispatch(err)); // , dispatch));
    })
    .catch((err) => errorDispatch(err)); // , dispatch));
}
