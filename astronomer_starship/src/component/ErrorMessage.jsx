import {
  Alert, AlertDescription, AlertIcon, AlertTitle,
} from '@chakra-ui/react';
import PropTypes from 'prop-types';
import React from 'react';

export default function ErrorMessage({ error }) {
  return error ? (
    <Alert status="error">
      <AlertIcon />
      <AlertTitle>Error Fetching Local Data!</AlertTitle>
      <AlertDescription>{error.message}</AlertDescription>
    </Alert>
  ) : null;
}
// eslint-disable-next-line react/forbid-prop-types
ErrorMessage.propTypes = { error: PropTypes.object };
ErrorMessage.defaultProps = { error: null };
