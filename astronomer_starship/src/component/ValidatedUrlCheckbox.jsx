import { Checkbox, useBoolean, useToast } from '@chakra-ui/react';
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
    axios.get(proxyUrl(url), { headers: proxyHeaders(token) })
      .then((res) => {
        // Valid if it's a 200, has data, and is JSON
        const isValid = (
          res.status === 200
          && res.data
          && res.headers['content-type'] === 'application/json'
        );
        setValid(isValid);
      })
      .catch((err) => {
        if (err.response.status === 404) {
          toast({
            title: 'Not found',
            status: 'error',
            isClosable: true,
          });
        } else {
          toast({
            title: err.response?.data?.error || err.message || err.response?.data,
            status: 'error',
            isClosable: true,
          });
        }
        setValid(false);
      })
      .finally(() => setLoading.off());
  }, [url, token]);

  return (
    <Checkbox
      isReadOnly
      isInvalid={!valid}
      isChecked={!loading && valid}
      // eslint-disable-next-line react/jsx-props-no-spreading
      {...props}
    >
      {text}
    </Checkbox>
  );
}
ValidatedUrlCheckbox.propTypes = {
  text: PropTypes.string.isRequired,
  url: PropTypes.string.isRequired,
  valid: PropTypes.bool.isRequired,
  setValid: PropTypes.func.isRequired,
  token: PropTypes.string.isRequired,
};
