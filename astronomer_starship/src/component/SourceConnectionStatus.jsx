import PropTypes from 'prop-types';
import { useCallback, useEffect, useState } from 'react';
import axios from 'axios';
import { Alert, AlertIcon, Box, Button, Code, HStack, Link, Text, VStack, useToast } from '@chakra-ui/react';
import { ExternalLinkIcon } from '@chakra-ui/icons';
import constants from '../constants';
import { localRoute } from '../util';
import ValidatedUrlCheckbox from './ValidatedUrlCheckbox';

/**
 * Validates the source Airflow connection and, on success, saves it as an
 * Airflow Connection (`starship_source`) via the Starship API.
 *
 * For platforms whose auth isn't browser-usable (GCC ADC, MWAA IAM, OSS
 * Basic), we skip the proxy probe and mark the checks as "ready" once the
 * URL is valid — actual connectivity is verified server-side at wave launch.
 */
export default function SourceConnectionStatus({
  platform = null,
  connId = 'starship_source',
  url = '',
  token = null,
  login = null,
  password = null,
  impersonationChain = '',
  region = '',
  roleArn = '',
  environmentName = '',
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

  const effectiveConnId = (connId && connId.trim()) || 'starship_source';

  const buildPayload = useCallback(() => {
    const payload = { platform, url, conn_id: effectiveConnId };
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
  }, [platform, url, effectiveConnId, token, login, password, impersonationChain, region, roleArn, environmentName]);

  const handleSave = async () => {
    setSaving(true);
    try {
      const res = await axios.post(localRoute(constants.SOURCE_CONNECTION_ROUTE), buildPayload());
      onConnectionSavedChange(true);
      const action = res.data?.action === 'updated' ? 'updated' : 'created';
      const savedId = res.data?.conn_id || effectiveConnId;
      toast({
        title: action === 'updated' ? 'Source connection updated' : 'Source connection created',
        description:
          action === 'updated'
            ? `Airflow Connection ${savedId} was updated with the current credentials. Cutover is ready to use.`
            : `Airflow Connection ${savedId} was created. Cutover tabs are now enabled above.`,
        status: 'success',
        duration: 5000,
        isClosable: true,
        variant: 'outline',
      });
    } catch (err) {
      const data = err.response?.data || {};
      const msg = data.error_message || data.error || err.message || 'Unknown error';
      toast({
        title: 'Failed to save source connection',
        description: typeof msg === 'string' ? msg : JSON.stringify(msg),
        status: 'error',
        duration: 8000,
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
            {/* Probes /api/starship/dags (not /info) on purpose: /info is a
                static ping that returns 200 even when the token lacks DAG
                read permissions, which would false-positive this checkbox
                and make Launch fail with a 403 on the first /dags call. */}
            <ValidatedUrlCheckbox
              colorScheme="success"
              text="Starship Plugin (DAG access)"
              valid={isStarship}
              setValid={onStarshipChange}
              url={`${url}/api/starship/dags`}
              token={token}
            />
          </>
        ) : (
          <Alert status="info" variant="left-accent" fontSize="xs" py={2}>
            <AlertIcon boxSize={3} />
            {platform === 'gcc' &&
              'Composer ADC cannot be verified from the browser — connectivity is checked server-side at wave launch.'}
            {platform === 'mwaa' &&
              'MWAA IAM cannot be verified from the browser — connectivity is checked server-side at wave launch.'}
            {platform === 'oss' &&
              'HTTP Basic cannot be verified from the browser — connectivity is checked server-side at wave launch.'}
          </Alert>
        )}
      </VStack>

      <Alert status={connectionSaved ? 'success' : 'info'} variant="subtle" fontSize="xs" borderRadius="md" mb={2}>
        <AlertIcon boxSize={3} />
        <Box>
          {connectionSaved ? (
            <>
              <Text>
                Saved as Airflow Connection <Code fontSize="2xs">{effectiveConnId}</Code>. Clicking below will update it
                with any changes you&apos;ve made above.
              </Text>
              <Text color="gray.600" mt={1}>
                Note: if you open this connection in Admin → Connections, Airflow shows the Password field blank —
                that&apos;s a deliberate UI safeguard, the token is stored and used at runtime.
              </Text>
            </>
          ) : (
            <Text>
              Clicking below will <strong>create or update</strong> an Airflow Connection named{' '}
              <Code fontSize="2xs">{effectiveConnId}</Code> with these credentials. The wave engine and the template DAG
              read from this connection at runtime.
            </Text>
          )}
        </Box>
      </Alert>
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
  connId: PropTypes.string,
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
