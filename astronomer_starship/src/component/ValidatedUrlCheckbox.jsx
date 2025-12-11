import {
  Checkbox, useBoolean, useToast, HStack, Spinner,
} from '@chakra-ui/react';
import React, { useEffect } from 'react';
import axios from 'axios';
import PropTypes from 'prop-types';
import { proxyHeaders, proxyUrl } from '../util';

export default function ValidatedUrlCheckbox({
  text, url, valid, setValid, token, ...props
}) {
  const [loading, setLoading] = useBoolean(true);
  const toast = useToast();
  useEffect(() => {
    // noinspection JSCheckFunctionSignatures
    axios
      .get(proxyUrl(url), { headers: proxyHeaders(token) })
      .then((res) => {
        // Valid if it's a 200, has data, and is JSON
        const isValid = res.status === 200 && res.data && res.headers['content-type'] === 'application/json';
        setValid(isValid);
      })
      .catch((err) => {
        if (err.response?.status === 404) {
          const resourceName = text === 'Airflow' ? 'Airflow API' : text === 'Starship' ? 'Starship API' : text;
          toast({
            title: `${resourceName} endpoint not found`,
            description: `Unable to reach ${resourceName} at the specified URL. Please verify the URL is correct and that ${text} is accessible.`,
            status: 'error',
            isClosable: true,
            duration: 5000,
          });
        } else {
          const errorMessage = err.response?.data?.error || err.message || err.response?.data || 'Unknown error';
          toast({
            title: `Failed to validate ${text}`,
            description: typeof errorMessage === 'string' ? errorMessage : JSON.stringify(errorMessage),
            status: 'error',
            isClosable: true,
            duration: 5000,
          });
        }
        setValid(false);
      })
      .finally(() => setLoading.off());
  }, [url, token]);

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
}
ValidatedUrlCheckbox.propTypes = {
  text: PropTypes.string.isRequired,
  url: PropTypes.string.isRequired,
  valid: PropTypes.bool.isRequired,
  setValid: PropTypes.func.isRequired,
  token: PropTypes.string.isRequired,
};
