import {
  Checkbox, useBoolean, useToast, HStack, Spinner,
} from '@chakra-ui/react';
import React, { useEffect, memo } from 'react';
import axios from 'axios';
import PropTypes from 'prop-types';
import { proxyHeaders, proxyUrl } from '../util';

/**
 * Generates user-friendly error messages based on the error type and context
 */
function getResourceName(text) {
  if (text === 'Airflow API') return 'Airflow API';
  if (text === 'Starship Plugin') return 'Starship Plugin API';
  return text;
}

function getErrorDetails(err, text, url) {
  const resourceName = getResourceName(text);
  const status = err.response?.status;
  const statusText = err.response?.statusText;

  // Network errors (no response received)
  if (err.code === 'ERR_NETWORK' || err.message === 'Network Error') {
    return {
      title: `Network error connecting to ${resourceName}`,
      description: 'Unable to reach the server. This could be due to:\n• The URL is incorrect\n• The server is not running\n• A firewall or network issue is blocking the connection\n• CORS policy is blocking the request',
    };
  }

  // Timeout errors
  if (err.code === 'ECONNABORTED' || err.message?.includes('timeout')) {
    return {
      title: `Connection timeout for ${resourceName}`,
      description: 'The request took too long to complete. The server may be overloaded or unreachable. Please try again.',
    };
  }

  // HTTP status-specific errors
  switch (status) {
    case 400:
      return {
        title: `Bad request to ${resourceName}`,
        description: `The server rejected the request. ${err.response?.data?.error || err.response?.data?.message || 'Please verify the URL format is correct.'}`,
      };

    case 401:
      return {
        title: `Authentication failed for ${resourceName}`,
        description: 'The provided token is invalid or expired. Please check your authentication token and ensure it has the required permissions.',
      };

    case 403:
      return {
        title: `Access denied to ${resourceName}`,
        description: 'The token is valid but lacks permission to access this resource. Please verify the token has the correct scope and permissions for this deployment.',
      };

    case 404:
      if (text === 'Starship Plugin') {
        return {
          title: 'Starship Plugin not found',
          description: 'The Starship plugin is not installed or enabled on the target Airflow instance. Please ensure astronomer-starship is installed and the webserver has been restarted.',
        };
      }
      return {
        title: `${resourceName} endpoint not found`,
        description: `The endpoint at "${url}" was not found. Please verify:\n• The URL is correct\n• The Airflow webserver is running\n• The API is accessible`,
      };

    case 500:
      return {
        title: `Server error from ${resourceName}`,
        description: `The server encountered an internal error. ${err.response?.data?.error || err.response?.data?.message || 'Please check the target Airflow logs for more details.'}`,
      };

    case 502:
      return {
        title: `Bad gateway for ${resourceName}`,
        description: 'The server received an invalid response from an upstream server. The Airflow webserver may be starting up or experiencing issues.',
      };

    case 503:
      return {
        title: `${resourceName} unavailable`,
        description: 'The service is temporarily unavailable. The Airflow webserver may be starting up, overloaded, or under maintenance.',
      };

    case 504:
      return {
        title: `Gateway timeout for ${resourceName}`,
        description: 'The server timed out waiting for a response. Please try again or check if the Airflow instance is responsive.',
      };

    default: {
      // Generic error with as much detail as possible
      const errorMessage = err.response?.data?.error
        || err.response?.data?.message
        || err.response?.data?.detail
        || err.message
        || (typeof err.response?.data === 'string' ? err.response?.data : null);

      let description;
      if (errorMessage) {
        description = typeof errorMessage === 'string'
          ? errorMessage
          : JSON.stringify(errorMessage);
      } else {
        let statusInfo = '';
        if (status) {
          statusInfo = statusText ? ` (HTTP ${status}: ${statusText})` : ` (HTTP ${status})`;
        }
        description = `An unexpected error occurred${statusInfo}. Please check the URL and try again.`;
      }

      return {
        title: `Failed to validate ${resourceName}`,
        description,
      };
    }
  }
}

const ValidatedUrlCheckbox = memo(({
  text, url, valid, setValid, token, ...props
}) => {
  const [loading, setLoading] = useBoolean(true);
  const toast = useToast();
  useEffect(() => {
    // noinspection JSCheckFunctionSignatures
    axios
      .get(proxyUrl(url), { headers: proxyHeaders(token), timeout: 30000 })
      .then((res) => {
        // Valid if it's a 200, has data, and is JSON
        const contentType = res.headers['content-type'] || '';
        const isJson = contentType.includes('application/json');
        const isValid = res.status === 200 && res.data && isJson;

        if (!isValid && res.status === 200) {
          // Got a 200 but response isn't what we expected
          toast({
            title: `Unexpected response from ${getResourceName(text)}`,
            description: !isJson
              ? 'The server returned a non-JSON response. You may have reached a login page or proxy. Please verify the URL and authentication.'
              : 'The server returned an empty or invalid response. Please verify the endpoint is correct.',
            status: 'warning',
            isClosable: true,
            duration: 4000,
            variant: 'outline',
          });
        }

        setValid(isValid);
      })
      .catch((err) => {
        const { title, description } = getErrorDetails(err, text, url);
        toast({
          title,
          description,
          status: 'error',
          isClosable: true,
          duration: 4000,
          variant: 'outline',
        });
        setValid(false);
      })
      .finally(() => setLoading.off());
  }, [url, token, text]);

  return (
    <HStack spacing={2}>
      <Checkbox
        isReadOnly
        isInvalid={!valid}
        isChecked={!loading && valid}
        cursor="default"
        // eslint-disable-next-line react/jsx-props-no-spreading
        {...props}
      >
        {text}
      </Checkbox>
      {loading && <Spinner size="sm" color="brand.400" thickness="2px" speed="0.8s" emptyColor="gray.200" />}
    </HStack>
  );
});

ValidatedUrlCheckbox.propTypes = {
  text: PropTypes.string.isRequired,
  url: PropTypes.string.isRequired,
  valid: PropTypes.bool.isRequired,
  setValid: PropTypes.func.isRequired,
  token: PropTypes.string.isRequired,
};

ValidatedUrlCheckbox.displayName = 'ValidatedUrlCheckbox';

export default ValidatedUrlCheckbox;
