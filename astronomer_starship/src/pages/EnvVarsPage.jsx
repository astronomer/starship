import React, { useEffect, useState, useCallback } from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import {
  Button, HStack, Text, useToast, Box, Heading, VStack, Stack,
} from '@chakra-ui/react';
import PropTypes from 'prop-types';
import axios from 'axios';
import { MdErrorOutline } from 'react-icons/md';
import { FaCheck } from 'react-icons/fa';
import { GoUpload } from 'react-icons/go';
import { RepeatIcon } from '@chakra-ui/icons';

import { useAppDispatch, useTargetConfig } from '../AppContext';
import ProgressSummary from '../component/ProgressSummary';
import DataTable from '../component/DataTable';
import PageLoading from '../component/PageLoading';
import HiddenValue from '../component/HiddenValue';
import {
  getAstroEnvVarRoute,
  getHoustonRoute,
  localRoute,
  proxyHeaders,
  proxyUrl,
} from '../util';
import constants, { getDeploymentsQuery, updateDeploymentVariablesMutation } from '../constants';

const columnHelper = createColumnHelper();

// Custom migrate button for env vars (different API pattern)
function EnvVarMigrateButton({
  route,
  headers = {},
  existsInRemote = false,
  sendData,
  isAstro,
  deploymentId = null,
  releaseName = null,
  onStatusChange = null,
}) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const toast = useToast();
  const [exists, setExists] = useState(existsInRemote);

  const handleError = (err) => {
    setExists(false);
    setLoading(false);
    toast({
      title: err.response?.data?.error || err.response?.data || err.message,
      status: 'error',
      isClosable: true,
      variant: 'outline',
      duration: 4000,
    });
    setError(err);
  };

  const handleSoftwareClick = () => {
    setLoading(true);
    axios
      .post(route, {
        operationName: 'deploymentVariables',
        query: getDeploymentsQuery,
        variables: { deploymentUuid: deploymentId, releaseName },
      }, { headers })
      .then((fetchRes) => {
        const variables = fetchRes.data?.data?.deploymentVariables || [];
        variables.push(sendData);
        return axios.post(route, {
          operationName: 'UpdateDeploymentVariables',
          query: updateDeploymentVariablesMutation,
          variables: { deploymentUuid: deploymentId, releaseName, environmentVariables: variables },
        }, { headers });
      })
      .then((updateRes) => {
        setLoading(false);
        const newStatus = updateRes.status === 200;
        setExists(newStatus);
        if (onStatusChange) onStatusChange(newStatus);
      })
      .catch(handleError);
  };

  const handleAstroClick = () => {
    setLoading(true);
    axios
      .get(route, { headers })
      .then((fetchRes) => {
        fetchRes.data?.environmentVariables.push(sendData);
        return axios.post(route, fetchRes.data, { headers });
      })
      .then((postRes) => {
        setLoading(false);
        const newStatus = postRes.status === 200;
        setExists(newStatus);
        if (onStatusChange) onStatusChange(newStatus);
      })
      .catch(handleError);
  };

  /* eslint-disable no-nested-ternary */
  return (
    <Button
      size="sm"
      variant="outline"
      isDisabled={loading || exists}
      isLoading={loading}
      loadingText="Loading"
      leftIcon={error ? <MdErrorOutline /> : exists ? <FaCheck /> : !loading ? <GoUpload /> : <span />}
      colorScheme={exists ? 'green' : error ? 'red' : 'green'}
      onClick={() => (isAstro ? handleAstroClick() : handleSoftwareClick())}
    >
      {exists ? 'Ok' : error ? 'Error!' : 'Migrate'}
    </Button>
  );
  /* eslint-enable no-nested-ternary */
}

EnvVarMigrateButton.propTypes = {
  route: PropTypes.string.isRequired,
  headers: PropTypes.objectOf(PropTypes.string),
  existsInRemote: PropTypes.bool,
  isAstro: PropTypes.bool.isRequired,
  sendData: PropTypes.shape({
    key: PropTypes.string,
    value: PropTypes.string,
    isSecret: PropTypes.bool,
  }).isRequired,
  deploymentId: PropTypes.string,
  releaseName: PropTypes.string,
  onStatusChange: PropTypes.func,
};


function mergeData(localData, remoteData) {
  return Object.entries(localData)
    .map(([key, value]) => ({ key, value }))
    .map((item) => ({ ...item, exists: item.key in remoteData }));
}

export default function EnvVarsPage() {
  const {
    targetUrl, token, isAstro, organizationId, deploymentId, releaseName, urlOrgPart,
  } = useTargetConfig();
  const dispatch = useAppDispatch();
  const toast = useToast();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState([]);
  const [isMigratingAll, setIsMigratingAll] = useState(false);

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const [localRes, remoteRes] = await Promise.all([
        axios.get(localRoute(constants.ENV_VAR_ROUTE)),
        axios.get(proxyUrl(targetUrl + constants.ENV_VAR_ROUTE), {
          headers: proxyHeaders(token),
        }),
      ]);

      if (localRes.status === 200 && remoteRes.status === 200) {
        setData(mergeData(localRes.data, remoteRes.data));
        // Extract org/deployment IDs from remote data
        if (remoteRes.data.ASTRO_ORGANIZATION_ID || remoteRes.data.ASTRO_DEPLOYMENT_ID) {
          dispatch({
            type: 'set-organization-id',
            organizationId: remoteRes.data.ASTRO_ORGANIZATION_ID,
            deploymentId: remoteRes.data.ASTRO_DEPLOYMENT_ID,
          });
        }
      } else {
        throw new Error('Invalid response from server');
      }
    } catch (err) {
      setError(err);
      if (err.response?.status === 401) {
        dispatch({ type: 'invalidate-token' });
      }
    } finally {
      setLoading(false);
    }
  }, [targetUrl, token, dispatch]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handleItemStatusChange = useCallback((key, newStatus) => {
    setData((prev) => prev.map((item) => (item.key === key ? { ...item, exists: newStatus } : item)));
  }, []);

  const handleMigrateAll = useCallback(() => {
    toast({
      title: 'Bulk migration not yet supported for Environment Variables',
      description: 'Please migrate environment variables individually',
      status: 'info',
      duration: 4000,
      isClosable: true,
      variant: 'outline',
    });
    setIsMigratingAll(false);
  }, [toast]);

  const columns = React.useMemo(() => [
    columnHelper.accessor('key', { header: 'Key' }),
    columnHelper.accessor('value', {
      header: 'Value',
      cell: ({ getValue }) => <HiddenValue value={getValue()} />,
    }),
    columnHelper.display({
      id: 'migrate',
      header: 'Migrate',
      meta: { align: 'right' },
      cell: ({ row }) => (
        <EnvVarMigrateButton
          route={isAstro
            ? proxyUrl(getAstroEnvVarRoute(organizationId, deploymentId))
            : proxyUrl(getHoustonRoute(urlOrgPart))}
          headers={proxyHeaders(token)}
          existsInRemote={row.original.exists}
          sendData={{ key: row.original.key, value: row.original.value, isSecret: false }}
          isAstro={isAstro}
          deploymentId={deploymentId}
          releaseName={releaseName}
          onStatusChange={(newStatus) => handleItemStatusChange(row.original.key, newStatus)}
        />
      ),
    }),
  ], [isAstro, organizationId, deploymentId, urlOrgPart, token, releaseName, handleItemStatusChange]);

  const totalItems = data.length;
  const migratedItems = data.filter((item) => item.exists).length;

  return (
    <Box>
      <Stack
        direction={{ base: 'column', md: 'row' }}
        justify="space-between"
        align={{ base: 'flex-start', md: 'center' }}
        mb={3}
      >
        <Box>
          <Heading size="md" mb={0.5}>Environment Variables</Heading>
          <Text fontSize="xs" color="gray.600">
            Environment variables for Airflow configurations and DAG access.
          </Text>
        </Box>
        <HStack>
          <Button
            leftIcon={<RepeatIcon />}
            onClick={fetchData}
            variant="outline"
            isLoading={loading}
          >
            Refresh
          </Button>
        </HStack>
      </Stack>

      <VStack spacing={3} align="stretch" w="100%">
        {!loading && !error && (
          <ProgressSummary
            totalItems={totalItems}
            migratedItems={migratedItems}
            onMigrateAll={handleMigrateAll}
            isMigratingAll={isMigratingAll}
          />
        )}

        <Box>
          {loading || error ? (
            <PageLoading loading={loading} error={error} />
          ) : (
            <DataTable data={data} columns={columns} searchPlaceholder="Search environment variables..." />
          )}
        </Box>
      </VStack>
    </Box>
  );
}
