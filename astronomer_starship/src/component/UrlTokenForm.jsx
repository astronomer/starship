import React from 'react';
import PropTypes from 'prop-types';
import {
  Box,
  Code,
  Divider,
  FormControl,
  FormErrorMessage,
  FormHelperText,
  FormLabel,
  HStack,
  Input,
  InputGroup,
  InputRightElement,
  Link,
  Text,
  Tooltip,
  VStack,
} from '@chakra-ui/react';
import { CheckIcon, ExternalLinkIcon, InfoIcon } from '@chakra-ui/icons';

import { parseAirflowUrl, tokenUrlFromAirflowUrl } from '../util';

/**
 * URL and Token input form for configuring the target Airflow instance.
 * Handles both Astro and Software URL formats with auto-detection.
 */
export default function UrlTokenForm({
  targetUrl,
  token = null,
  isAstro,
  isTouched,
  isTokenTouched,
  isValidUrl,
  onUrlChange,
  onTokenChange,
  onToggleIsAstro,
}) {
  const handleUrlChange = (e) => {
    const parsed = parseAirflowUrl(e.target.value);
    // Auto-detect product type if URL is valid
    if (parsed.isValid && parsed.isAstro !== isAstro) {
      onToggleIsAstro();
    }
    onUrlChange({
      targetUrl: parsed.isValid ? parsed.targetUrl : e.target.value,
      urlOrgPart: parsed.urlOrgPart,
      urlDeploymentPart: parsed.urlDeploymentPart,
    });
  };

  return (
    <>
      <FormControl
        className="setup-form-field"
        isInvalid={isTouched && !isValidUrl}
        isRequired
      >
        <HStack mb={1}>
          <FormLabel mb={0}>Airflow URL</FormLabel>
          <Tooltip
            label="Copy the webserver URL from your Astronomer deployment"
            placement="top"
            hasArrow
          >
            <InfoIcon color="gray.400" boxSize={3} cursor="help" />
          </Tooltip>
        </HStack>
        <Input
          size="sm"
          placeholder="https://..."
          value={targetUrl}
          isInvalid={isTouched && !isValidUrl}
          onChange={handleUrlChange}
        />
        <FormHelperText fontSize="xs">
          Paste the full webserver URL of your target Airflow deployment
        </FormHelperText>
        <FormErrorMessage>Please enter a valid Airflow URL</FormErrorMessage>
      </FormControl>

      {/* URL Format Guidance */}
      <Box
        mt={3}
        p={3}
        bg="gray.50"
        borderRadius="md"
        borderWidth="1px"
        borderColor="gray.200"
      >
        <Text fontSize="xs" fontWeight="semibold" color="gray.600" mb={2}>
          Supported URL Formats
        </Text>
        <VStack align="stretch" spacing={2}>
          <Box>
            <Text fontSize="2xs" color="gray.500" mb={0.5}>
              Astro (Cloud)
            </Text>
            <Code fontSize="2xs" px={2} py={1} borderRadius="sm" display="block">
              https://claaabbbcccddd.astronomer.run/aabbccdd
            </Code>
          </Box>
          <Box>
            <Text fontSize="2xs" color="gray.500" mb={0.5}>
              Astro Private Cloud (Software)
            </Text>
            <Code fontSize="2xs" px={2} py={1} borderRadius="sm" display="block">
              https://deployments.basedomain.com/release-name-1234/airflow
            </Code>
          </Box>
        </VStack>
      </Box>

      <Divider my={3} />

      <FormControl
        isInvalid={isTokenTouched && !token}
        className="setup-form-field"
      >
        <HStack mb={1}>
          <FormLabel mb={0}>Authentication Token</FormLabel>
          <Tooltip
            label={
              isAstro
                ? 'Use an Organization, Workspace, or Personal access token'
                : 'Use a Workspace or Deployment service account token'
            }
            placement="top"
            hasArrow
          >
            <InfoIcon color="gray.400" boxSize={3} cursor="help" />
          </Tooltip>
        </HStack>
        <InputGroup size="sm">
          <Input
            type="password"
            autoComplete="off"
            value={token || ''}
            isInvalid={isTokenTouched && !token}
            placeholder="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
            onChange={(e) => onTokenChange(e.target.value)}
          />
          {isTokenTouched && token && (
            <InputRightElement>
              <CheckIcon color="success.500" />
            </InputRightElement>
          )}
        </InputGroup>
        {isAstro ? (
          <FormHelperText>
            Provide a token:
            {' '}
            <Tooltip
              label="Organization tokens have access to all workspaces and deployments"
              hasArrow
            >
              <Link
                isExternal
                href="https://docs.astronomer.io/astro/organization-api-tokens"
              >
                Organization
                <ExternalLinkIcon mx="2px" />
              </Link>
            </Tooltip>
            ,
            {' '}
            <Tooltip
              label="Workspace tokens have access to all deployments in a workspace"
              hasArrow
            >
              <Link
                isExternal
                href="https://docs.astronomer.io/astro/workspace-api-tokens"
              >
                Workspace
                <ExternalLinkIcon mx="2px" />
              </Link>
            </Tooltip>
            ,
            {' '}
            <Tooltip label="Personal tokens are user-specific access tokens" hasArrow>
              <Link isExternal href={tokenUrlFromAirflowUrl(targetUrl)}>
                Personal
                <ExternalLinkIcon mx="2px" />
              </Link>
            </Tooltip>
            .
          </FormHelperText>
        ) : (
          <FormHelperText>
            Provide a token:
            {' '}
            <Tooltip
              label="Workspace service accounts have access to all deployments"
              hasArrow
            >
              <Link
                isExternal
                href="https://docs.astronomer.io/software/manage-workspaces#service-accounts"
              >
                Workspace
                <ExternalLinkIcon mx="2px" />
              </Link>
            </Tooltip>
            ,
            {' '}
            <Tooltip
              label="Deployment service accounts are deployment-specific tokens"
              hasArrow
            >
              <Link
                isExternal
                href="https://docs.astronomer.io/software/ci-cd#step-1-create-a-service-account"
              >
                Deployment
                <ExternalLinkIcon mx="2px" />
              </Link>
            </Tooltip>
            {targetUrl.startsWith('https://') && isValidUrl && (
              <>
                ,
                {' '}
                <Tooltip
                  label="Personal tokens are user-specific access tokens"
                  hasArrow
                >
                  <Link isExternal href={tokenUrlFromAirflowUrl(targetUrl)}>
                    Personal
                    <ExternalLinkIcon mx="2px" />
                  </Link>
                </Tooltip>
              </>
            )}
            .
          </FormHelperText>
        )}
        <FormErrorMessage>
          Please input a valid authentication token
        </FormErrorMessage>
      </FormControl>
    </>
  );
}

UrlTokenForm.propTypes = {
  targetUrl: PropTypes.string.isRequired,
  token: PropTypes.string,
  isAstro: PropTypes.bool.isRequired,
  isTouched: PropTypes.bool.isRequired,
  isTokenTouched: PropTypes.bool.isRequired,
  isValidUrl: PropTypes.bool.isRequired,
  onUrlChange: PropTypes.func.isRequired,
  onTokenChange: PropTypes.func.isRequired,
  onToggleIsAstro: PropTypes.func.isRequired,
};
