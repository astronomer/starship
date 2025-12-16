import React, {
  useEffect, useState, useCallback, useMemo,
} from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import {
  Button, HStack, Text, useToast, Box, Heading, VStack, Stack,
} from '@chakra-ui/react';
import axios from 'axios';
import { RepeatIcon } from '@chakra-ui/icons';

import { useAppDispatch, useTargetConfig } from '../AppContext';
import ProgressSummary from '../component/ProgressSummary';
import DataTable from '../component/DataTable';
import PageLoading from '../component/PageLoading';
import HiddenValue from '../component/HiddenValue';
import EnvVarMigrateButton from '../component/EnvVarMigrateButton';
import {
  getAstroEnvVarRoute,
  getHoustonRoute,
  localRoute,
  proxyHeaders,
  proxyUrl,
} from '../util';
import constants from '../constants';

const columnHelper = createColumnHelper();

function mergeData(localData, remoteData) {
  return Object.entries(localData)
    .map(([key, value]) => ({ key, value }))
    .map((item) => ({ ...item, exists: item.key in remoteData }));
}

/**
 * Renders a hidden value cell.
 */
function renderHiddenValue(info) {
  return <HiddenValue value={info.getValue()} />;
}

/**
 * Creates column definitions for the environment variables table.
 * Defined outside component to avoid unstable nested components.
 */
function createColumns(config) {
  const {
    isAstro, organizationId, deploymentId, urlOrgPart,
    token, releaseName, handleItemStatusChange,
  } = config;

  return [
    columnHelper.accessor('key', { header: 'Key' }),
    columnHelper.accessor('value', {
      header: 'Value',
      cell: renderHiddenValue,
    }),
    columnHelper.display({
      id: 'migrate',
      header: 'Migrate',
      meta: { align: 'right' },
      cell: (info) => {
        const { original } = info.row;
        let route;
        if (isAstro) {
          route = proxyUrl(getAstroEnvVarRoute(organizationId, deploymentId));
        } else {
          route = proxyUrl(getHoustonRoute(urlOrgPart));
        }

        return (
          <EnvVarMigrateButton
            route={route}
            headers={proxyHeaders(token)}
            existsInRemote={original.exists}
            sendData={{
              key: original.key,
              value: original.value,
              isSecret: false,
            }}
            isAstro={isAstro}
            deploymentId={deploymentId}
            releaseName={releaseName}
            onStatusChange={(newStatus) => handleItemStatusChange(original.key, newStatus)}
          />
        );
      },
    }),
  ];
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
    setData((prev) => prev.map((item) => (
      item.key === key ? { ...item, exists: newStatus } : item
    )));
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

  const columns = useMemo(
    () => createColumns({
      isAstro,
      organizationId,
      deploymentId,
      urlOrgPart,
      token,
      releaseName,
      handleItemStatusChange,
    }),
    [isAstro, organizationId, deploymentId, urlOrgPart, token, releaseName, handleItemStatusChange],
  );

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
            <DataTable
              data={data}
              columns={columns}
              searchPlaceholder="Search environment variables..."
            />
          )}
        </Box>
      </VStack>
    </Box>
  );
}
