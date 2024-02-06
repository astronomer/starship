import {
  Alert, AlertDescription, AlertIcon, AlertTitle, Center, Spinner,
} from '@chakra-ui/react';
import React from 'react';
import PropTypes from 'prop-types';

export default function PageLoading({ loading, error }) {
  // eslint-disable-next-line no-nested-ternary
  return loading ? (
    <Center margin="0 auto" height="75%">
      <Spinner
        thickness="6px"
        speed="0.5s"
        emptyColor="gray.200"
        color="blue.500"
        size="xl"
      />
    </Center>
  ) : error ? (
    <Alert status="error">
      <AlertIcon />
      <AlertTitle>Error Fetching Local Data!</AlertTitle>
      <AlertDescription>{error.message}</AlertDescription>
    </Alert>
  ) : null;
}
// eslint-disable-next-line react/forbid-prop-types
PageLoading.propTypes = { loading: PropTypes.bool.isRequired, error: PropTypes.object };
PageLoading.defaultProps = { error: null };
