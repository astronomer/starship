import PropTypes from 'prop-types';
import { useCallback, useEffect, useState } from 'react';
import axios from 'axios';
import { Alert, AlertIcon, Box, Button, HStack, Link, Text, VStack, useToast } from '@chakra-ui/react';
import { ExternalLinkIcon } from '@chakra-ui/icons';
import ValidatedUrlCheckbox from './ValidatedUrlCheckbox';
import constants from '../constants';
import { localRoute } from '../util';

/**
 * Validates the source Airflow connection and, on success, saves it as an
 * Airflow Connection (`starship_source`) via the Starship API.
 *
 * For platforms whose auth isn't browser-usable (GCC ADC, MWAA IAM, OSS
 * Basic), we skip the proxy probe and mark the checks as "ready" once the
 * URL is valid — actual connectivity is verified server-side at wave launch.
 */
export default function SourceConnectionStatus({
  platform,
  url,
  token,
  login,
  password,
  impersonationChain,
  region,
  roleArn,
  environmentName,
  isValidUrl,
  isAirflow,
  isStarship,
  connectionSaved,
  hasCreds,
  onAirflowChange,
  onStarshipChange,
  onConnectionSavedChange,
}) {
  const toast = useToast();
  const [saving, setSaving] = useState(false);

  const isReady = !!platform && isValidUrl && hasCreds;
  const canProxyProbe = (platform === 'astro' && !!token) || (platform === 'oss' && !!token);

  // For non-proxy-probe platforms, auto-mark connectivity as ready once
  // the URL is valid and creds are in place.
  useEffect(() => {
    if (!isReady) return;
    if (!canProxyProbe) {
      if (!isAirflow) onAirflowChange(true);
      if (!isStarship) onStarshipChange(true);
    }
  }, [isReady, canProxyProbe, isAirflow, isStarship, onAirflowChange, onStarshipChange]);

  const buildPayload = useCallback(() => {
    const payload = { platform, url };
    if (platform === 'astro') {
      payload.token = token;
    } else if (platform === 'oss') {
      if (token) {
        payload.token = token;
      } else {
        payload.login = login;
        payload.password = password;
      }
    } else if (platform === 'gcc') {
      const chain = (impersonationChain || '')
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean);
      if (chain.length) payload.impersonation_chain = chain;
    } else if (platform === 'mwaa') {
      payload.region = region;
      if (roleArn) payload.role_arn = roleArn;
      if (environmentName) payload.environment_name = environmentName;
    }
    return payload;
  }, [platform, url, token, login, password, impersonationChain, region, roleArn, environmentName]);

  const handleSave = async () => {
    setSaving(true);
    try {
      await axios.post(localRoute(constants.SOURCE_CONNECTION_ROUTE), buildPayload());
      onConnectionSavedChange(true);
      toast({
        title: 'Source connection saved',
        description: 'Cutover tabs are now enabled in the navigation above.',
        status: 'success',
        duration: 4000,
        isClosable: true,
        variant: 'outline',
      });
    } catch (err) {
      const msg = err.response?.data?.error || err.message || 'Unknown error';
      toast({
        title: 'Failed to save source connection',
        description: typeof msg === 'string' ? msg : JSON.stringify(msg),
        status: 'error',
        duration: 6000,
        isClosable: true,
        variant: 'outline',
      });
    } finally {
      setSaving(false);
    }
  };

  if (!isReady) {
    return (
      <Text fontSize="sm" color="gray.500" fontStyle="italic">
        Pick a platform and fill in the Source Airflow form to verify the connection.
      </Text>
    );
  }

  return (
    <Box>
      <Text fontSize="xs" color="gray.600" mb={1.5}>
        Verifying connectivity to your source Airflow instance
      </Text>
      <Box mb={2} p={2} bg="gray.50" borderRadius="md">
        <Text fontSize="2xs" color="gray.500" mb={0.5}>
          Source URL
        </Text>
        <Link
          isExternal
          href={url}
          color="brand.600"
          fontWeight="medium"
          wordBreak="break-all"
          overflowWrap="anywhere"
          display="inline"
        >
          {url}
          <ExternalLinkIcon mx="2px" verticalAlign="middle" />
        </Link>
      </Box>

      <VStack align="stretch" spacing={1.5} mb={3}>
        {canProxyProbe ? (
          <>
            <ValidatedUrlCheckbox
              colorScheme="success"
              text="Airflow API"
              valid={isAirflow}
              setValid={onAirflowChange}
              url={`${url}/api/v1/health`}
              token={token}
            />
            <ValidatedUrlCheckbox
              colorScheme="success"
              text="Starship Plugin"
              valid={isStarship}
              setValid={onStarshipChange}
              url={`${url}/api/starship/info`}
              token={token}
            />
          </>
        ) : (
          <Alert status="info" variant="left-accent" fontSize="xs" py={2}>
            <AlertIcon boxSize={3} />
            {platform === 'gcc' && 'Composer ADC cannot be verified from the browser — connectivity is checked server-side at wave launch.'}
            {platform === 'mwaa' && 'MWAA IAM cannot be verified from the browser — connectivity is checked server-side at wave launch.'}
            {platform === 'oss' && 'HTTP Basic cannot be verified from the browser — connectivity is checked server-side at wave launch.'}
          </Alert>
        )}
      </VStack>

      <HStack justify="flex-end">
        <Button
          size="sm"
          colorScheme="brand"
          onClick={handleSave}
          isLoading={saving}
          isDisabled={!isAirflow || !isStarship}
        >
          {connectionSaved ? 'Update Source Connection' : 'Save Source Connection'}
        </Button>
      </HStack>
    </Box>
  );
}

SourceConnectionStatus.propTypes = {
  platform: PropTypes.string,
  url: PropTypes.string,
  token: PropTypes.string,
  login: PropTypes.string,
  password: PropTypes.string,
  impersonationChain: PropTypes.string,
  region: PropTypes.string,
  roleArn: PropTypes.string,
  environmentName: PropTypes.string,
  isValidUrl: PropTypes.bool.isRequired,
  isAirflow: PropTypes.bool.isRequired,
  isStarship: PropTypes.bool.isRequired,
  connectionSaved: PropTypes.bool.isRequired,
  hasCreds: PropTypes.bool.isRequired,
  onAirflowChange: PropTypes.func.isRequired,
  onStarshipChange: PropTypes.func.isRequired,
  onConnectionSavedChange: PropTypes.func.isRequired,
};

SourceConnectionStatus.defaultProps = {
  platform: null,
  url: '',
  token: null,
  login: null,
  password: null,
  impersonationChain: '',
  region: '',
  roleArn: '',
  environmentName: '',
};
