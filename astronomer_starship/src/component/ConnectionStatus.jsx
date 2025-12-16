import React from 'react';
import PropTypes from 'prop-types';
import {
  Box,
  Link,
  Text,
  VStack,
} from '@chakra-ui/react';
import { ExternalLinkIcon } from '@chakra-ui/icons';

import ValidatedUrlCheckbox from './ValidatedUrlCheckbox';

/**
 * Connection status validation component.
 * Validates connectivity to both the Airflow API and Starship plugin.
 */
export default function ConnectionStatus({
  targetUrl,
  token = null,
  isValidUrl,
  isAirflow,
  isStarship,
  onAirflowChange,
  onStarshipChange,
}) {
  const isReady = targetUrl.startsWith('http') && isValidUrl && token;

  if (!isReady) {
    return (
      <Text fontSize="sm" color="gray.500" fontStyle="italic">
        Complete Step 1 to verify connections
      </Text>
    );
  }

  return (
    <Box>
      <Text fontSize="xs" color="gray.600" mb={1.5}>
        Verifying connectivity to your target Airflow instance
      </Text>
      <Box mb={1.5} p={2} bg="gray.50" borderRadius="md">
        <Text fontSize="2xs" color="gray.500" mb={0.5}>
          Target URL
        </Text>
        <Link
          isExternal
          href={targetUrl}
          color="brand.600"
          fontWeight="medium"
          wordBreak="break-all"
          overflowWrap="anywhere"
          display="inline"
        >
          {targetUrl}
          <ExternalLinkIcon mx="2px" verticalAlign="middle" />
        </Link>
      </Box>
      <VStack align="stretch" spacing={1.5}>
        <ValidatedUrlCheckbox
          colorScheme="success"
          text="Airflow API"
          valid={isAirflow}
          setValid={onAirflowChange}
          url={`${targetUrl}/api/v1/health`}
          token={token}
        />
        <ValidatedUrlCheckbox
          colorScheme="success"
          text="Starship Plugin"
          valid={isStarship}
          setValid={onStarshipChange}
          url={`${targetUrl}/api/starship/info`}
          token={token}
        />
      </VStack>
    </Box>
  );
}

ConnectionStatus.propTypes = {
  targetUrl: PropTypes.string.isRequired,
  token: PropTypes.string,
  isValidUrl: PropTypes.bool.isRequired,
  isAirflow: PropTypes.bool.isRequired,
  isStarship: PropTypes.bool.isRequired,
  onAirflowChange: PropTypes.func.isRequired,
  onStarshipChange: PropTypes.func.isRequired,
};
