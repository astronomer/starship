import { Center, Spinner } from '@chakra-ui/react';
import React from 'react';
import PropTypes from 'prop-types';

export default function PageLoading({ loading }) {
  return loading ? (
    <Center>
      <Spinner
        thickness="6px"
        speed="0.5s"
        emptyColor="gray.200"
        color="blue.500"
        size="xl"
      />
    </Center>
  ) : null;
}
PageLoading.propTypes = { loading: PropTypes.bool.isRequired };
