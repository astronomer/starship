import React from 'react';
import {
  Alert,
  AlertDescription,
  AlertIcon,
  AlertTitle,
  Center,
  Spinner,
  VStack,
  Text,
} from '@chakra-ui/react';
import PropTypes from 'prop-types';

/**
 * Displays a loading spinner or error state for page data fetching
 */
export default function PageLoading({ loading = false, error = null }) {
  if (loading) {
    return (
      <Center py={12}>
        <VStack spacing={4}>
          <Spinner
            thickness="4px"
            speed="0.65s"
            emptyColor="gray.200"
            color="brand.400"
            size="xl"
          />
          <Text color="gray.500" fontSize="sm">Loading...</Text>
        </VStack>
      </Center>
    );
  }

  if (error) {
    return (
      <Alert status="error" borderRadius="md">
        <AlertIcon />
        <VStack align="start" spacing={0}>
          <AlertTitle>Error fetching data</AlertTitle>
          <AlertDescription>
            {error.message || 'An unexpected error occurred'}
          </AlertDescription>
        </VStack>
      </Alert>
    );
  }

  return null;
}

PageLoading.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.shape({
    message: PropTypes.string,
  }),
};
