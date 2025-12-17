import React, {
  useEffect, useState, useCallback, useMemo,
} from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import {
  Badge,
  Box,
  Button,
  FormControl,
  Heading,
  HStack,
  InputGroup,
  InputLeftAddon,
  Link,
  NumberDecrementStepper,
  NumberIncrementStepper,
  NumberInput,
  NumberInputField,
  NumberInputStepper,
  Stack,
  Switch,
  Tag,
  Text,
  Tooltip,
  useToast,
  VStack,
} from '@chakra-ui/react';
import axios from 'axios';
import humanFormat from 'human-format';
import { ExternalLinkIcon, RepeatIcon } from '@chakra-ui/icons';
import { FiPause, FiPlay } from 'react-icons/fi';

import { useAppDispatch, useTargetConfig, useDagHistoryConfig } from '../AppContext';
import DataTable from '../component/DataTable';
import PageLoading from '../component/PageLoading';
import TooltipHeader from '../component/TooltipHeader';
import DAGHistoryMigrateButton from '../component/DAGHistoryMigrateButton';
import {
  localRoute, proxyHeaders, proxyUrl, getDagViewPath,
} from '../util';
import mergeDagData from '../utils/dagUtils';
import constants from '../constants';

const columnHelper = createColumnHelper();

// Cell renderer functions - defined outside component to avoid unstable nested components
function renderDagId(info) {
  const { row, getValue } = info;
  return (
    <Tooltip hasArrow label={`File: ${row.original.local.fileloc}`}>
      <Text fontWeight="semibold">
        {getValue()}
      </Text>
    </Tooltip>
  );
}

function renderTags(info) {
  const tags = info.getValue();
  if (!tags || tags.length === 0) return null;
  return (
    <HStack spacing={1} flexWrap="wrap">
      {tags.map((tag) => (
        <Tag key={tag} size="sm" colorScheme="amethyst" variant="solid">
          {tag}
        </Tag>
      ))}
    </HStack>
  );
}

function renderSchedule(info) {
  return info.getValue() || 'None';
}

/**
 * Creates column definitions for the DAG history table.
 * Defined outside component to avoid unstable nested components.
 */
function createColumns(config) {
  const {
    targetUrl, token, limit, batchSize, handleMigrate,
    handleDelete, localAirflowVersion, handlePausedClick,
  } = config;

  return [
    columnHelper.accessor((row) => row.local.dag_id, {
      id: 'dagId',
      header: 'ID',
      cell: renderDagId,
    }),
    columnHelper.accessor((row) => row.local.tags, {
      id: 'tags',
      header: 'Tags',
      cell: renderTags,
    }),
    columnHelper.accessor((row) => row.local.schedule_interval, {
      id: 'schedule',
      header: 'Schedule',
      cell: renderSchedule,
    }),
    columnHelper.accessor((row) => row.local.description, {
      id: 'description',
      header: 'Description',
    }),
    columnHelper.accessor((row) => row.local.owners, {
      id: 'owners',
      header: 'Owners',
    }),
    columnHelper.display({
      id: 'local_is_paused',
      enableSorting: false,
      header: () => (
        <>
          Local
          <TooltipHeader tooltip="Toggle to pause/unpause DAG in local" />
        </>
      ),
      cell: (info) => {
        const { original } = info.row;
        return (
          <>
            <Switch
              colorScheme="success"
              isChecked={!original.local.is_paused}
              onChange={() => handlePausedClick(
                !original.local.is_paused,
                original.local.dag_id,
                true,
              )}
            />
            <Tooltip hasArrow label="DAG Run Count">
              <Badge
                mx={1}
                fontSize="sm"
                variant="outline"
                colorScheme={original.local.dag_run_count > 0 ? 'teal' : 'red'}
              >
                {humanFormat(original.local.dag_run_count)}
              </Badge>
            </Tooltip>
          </>
        );
      },
    }),
    columnHelper.display({
      id: 'local_url',
      header: 'Local URL',
      enableSorting: false,
      cell: (info) => {
        const { original } = info.row;
        return (
          <Link
            isExternal
            href={localRoute(getDagViewPath(original.local.dag_id, localAirflowVersion))}
            color="brand.700"
          >
            View DAG
            {' '}
            <ExternalLinkIcon mx="2px" />
          </Link>
        );
      },
    }),
    columnHelper.display({
      id: 'remote_is_paused',
      enableSorting: false,
      header: () => (
        <>
          Remote
          <TooltipHeader tooltip="Toggle to pause/unpause DAG in remote" />
        </>
      ),
      cell: (info) => {
        const { original } = info.row;
        if (!original.remote) return null;
        return (
          <>
            <Switch
              colorScheme="success"
              isChecked={!original.remote?.is_paused}
              onChange={() => handlePausedClick(
                !original.remote?.is_paused,
                original.local.dag_id,
                false,
              )}
            />
            <Tooltip hasArrow label="DAG Run Count">
              <Badge
                mx={1}
                fontSize="sm"
                variant="outline"
                colorScheme={original.remote?.dag_run_count > 0 ? 'teal' : 'red'}
              >
                {humanFormat(original.remote?.dag_run_count || 0)}
              </Badge>
            </Tooltip>
          </>
        );
      },
    }),
    columnHelper.display({
      id: 'remote_url',
      header: 'Remote URL',
      enableSorting: false,
      cell: (info) => {
        const { original } = info.row;
        if (!original.remote) return null;
        return (
          <Link
            isExternal
            href={`${targetUrl}/dags/${original.remote.dag_id}`}
            color="brand.700"
          >
            View DAG
            {' '}
            <ExternalLinkIcon mx="2px" />
          </Link>
        );
      },
    }),
    columnHelper.display({
      id: 'migrate',
      header: 'Migrate',
      meta: { align: 'right' },
      enableSorting: false,
      cell: (info) => {
        const { original } = info.row;

        // Determine disabled state
        let isDisabledReason = false;
        if (!original.remote?.dag_id) {
          isDisabledReason = 'DAG not found in remote';
        } else if (!original.local.dag_run_count && !original.remote?.dag_run_count) {
          isDisabledReason = 'No DAG Runs to migrate';
        }

        return (
          <DAGHistoryMigrateButton
            url={targetUrl}
            token={token}
            dagId={original.local.dag_id}
            limit={Number(limit)}
            batchSize={Number(batchSize)}
            existsInRemote={!!original.remote?.dag_run_count}
            isDisabled={isDisabledReason}
            onMigrate={handleMigrate}
            onDelete={handleDelete}
          />
        );
      },
    }),
  ];
}

export default function DAGHistoryPage() {
  const { targetUrl, token, localAirflowVersion } = useTargetConfig();
  const { limit, batchSize } = useDagHistoryConfig();
  const dispatch = useAppDispatch();
  const toast = useToast();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState([]);

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      // Fetch local Airflow version if not already known
      if (!localAirflowVersion) {
        const infoRes = await axios.get(localRoute('/api/starship/info'));
        if (infoRes.status === 200 && infoRes.data?.airflow_version) {
          dispatch({ type: 'set-local-airflow-version', version: infoRes.data.airflow_version });
        }
      }

      const [localRes, remoteRes] = await Promise.all([
        axios.get(localRoute(constants.DAGS_ROUTE)),
        axios.get(proxyUrl(targetUrl + constants.DAGS_ROUTE), { headers: proxyHeaders(token) }),
      ]);

      if (localRes.status === 200 && remoteRes.status === 200) {
        setData(mergeDagData(localRes.data, remoteRes.data));
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
  }, [targetUrl, token, dispatch, localAirflowVersion]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handlePausedClick = useCallback(async (isPaused, dagId, isLocal) => {
    const url = isLocal
      ? localRoute(constants.DAGS_ROUTE)
      : proxyUrl(targetUrl + constants.DAGS_ROUTE);
    try {
      const res = await axios.patch(
        url,
        { dag_id: dagId, is_paused: isPaused },
        { headers: proxyHeaders(token) },
      );
      setData((prev) => prev.map((item) => {
        if (item.local.dag_id !== dagId) return item;
        const key = isLocal ? 'local' : 'remote';
        return { ...item, [key]: { ...item[key], is_paused: res.data.is_paused } };
      }));
    } catch (err) {
      toast({
        title: 'Failed to update DAG pause state',
        description: err.message,
        status: 'error',
        isClosable: true,
        variant: 'outline',
        duration: 6000,
      });
    }
  }, [targetUrl, token, toast]);

  const handleMigrate = useCallback((dagId, runCount) => {
    setData((prev) => prev.map((item) => {
      if (item.local.dag_id !== dagId) return item;
      return { ...item, remote: { ...item.remote, dag_run_count: runCount } };
    }));
  }, []);

  const handleDelete = useCallback((dagId) => {
    setData((prev) => prev.map((item) => {
      if (item.local.dag_id !== dagId) return item;
      return { ...item, remote: { ...item.remote, dag_run_count: 0 } };
    }));
  }, []);

  const handleBulkPause = useCallback(async (isLocal, pause) => {
    const items = data.filter((item) => {
      const target = isLocal ? item.local : item.remote;
      return target && target.is_paused !== pause;
    });

    if (items.length === 0) {
      toast({
        title: 'No changes needed',
        description: `All ${isLocal ? 'local' : 'remote'} DAGs are already ${pause ? 'paused' : 'active'}`,
        status: 'info',
        duration: 4000,
        variant: 'outline',
      });
      return;
    }

    let successCount = 0;
    // eslint-disable-next-line no-restricted-syntax
    for (const item of items) {
      try {
        // eslint-disable-next-line no-await-in-loop
        await handlePausedClick(pause, item.local.dag_id, isLocal);
        successCount += 1;
      } catch (err) {
        // Continue on error
      }
    }

    const dagLabel = successCount !== 1 ? 'DAGs' : 'DAG';
    const locationLabel = isLocal ? 'local' : 'remote';
    const failedCount = items.length - successCount;
    toast({
      title: `${pause ? 'Paused' : 'Activated'} ${successCount} ${locationLabel} ${dagLabel}`,
      description: failedCount > 0
        ? `${failedCount} ${failedCount !== 1 ? 'DAGs' : 'DAG'} failed to update`
        : `Successfully updated ${locationLabel} Airflow instance`,
      status: failedCount > 0 ? 'warning' : 'success',
      duration: 4000,
      variant: 'outline',
    });
  }, [data, toast, handlePausedClick]);

  const columns = useMemo(
    () => createColumns({
      targetUrl,
      token,
      limit,
      batchSize,
      handleMigrate,
      handleDelete,
      localAirflowVersion,
      handlePausedClick,
    }),
    [
      targetUrl, token, limit, batchSize, handleMigrate,
      handleDelete, localAirflowVersion, handlePausedClick,
    ],
  );

  return (
    <Box>
      <Stack
        direction={{ base: 'column', md: 'row' }}
        justify="space-between"
        align={{ base: 'flex-start', md: 'center' }}
        mb={3}
      >
        <Box>
          <Heading size="md" mb={0.5}>DAG History</Heading>
          <Text fontSize="xs" color="gray.600">
            Migrate DAGs and task history to prevent rescheduling.
          </Text>
        </Box>
        <HStack spacing={2}>
          <Tooltip hasArrow label="Total DAG Runs to migrate">
            <FormControl minW="40">
              <InputGroup size="sm">
                <InputLeftAddon># DAG Runs</InputLeftAddon>
                <NumberInput
                  value={limit}
                  onChange={(val) => dispatch({ type: 'set-limit', limit: Number(val) })}
                >
                  <NumberInputField />
                  <NumberInputStepper>
                    <NumberIncrementStepper />
                    <NumberDecrementStepper />
                  </NumberInputStepper>
                </NumberInput>
              </InputGroup>
            </FormControl>
          </Tooltip>
          <Tooltip hasArrow label="DAG Runs per batch">
            <FormControl minW="40">
              <InputGroup size="sm">
                <InputLeftAddon>Batch Size</InputLeftAddon>
                <NumberInput
                  value={batchSize}
                  onChange={(val) => dispatch({ type: 'set-batch-size', batchSize: Number(val) })}
                >
                  <NumberInputField />
                  <NumberInputStepper>
                    <NumberIncrementStepper />
                    <NumberDecrementStepper />
                  </NumberInputStepper>
                </NumberInput>
              </InputGroup>
            </FormControl>
          </Tooltip>
          <Button
            size="sm"
            leftIcon={<RepeatIcon />}
            onClick={fetchData}
            variant="outline"
            isLoading={loading}
            flexShrink={0}
          >
            Refresh
          </Button>
        </HStack>
      </Stack>

      <HStack spacing={2} mb={3} justify="space-between">
        <HStack spacing={2}>
          <Text fontSize="sm" fontWeight="semibold" color="gray.600">Local:</Text>
          <Button
            size="sm"
            leftIcon={<FiPause />}
            onClick={() => handleBulkPause(true, true)}
            colorScheme="orange"
            variant="outline"
          >
            Pause All
          </Button>
          <Button
            size="sm"
            leftIcon={<FiPlay />}
            onClick={() => handleBulkPause(true, false)}
            colorScheme="green"
            variant="outline"
          >
            Unpause All
          </Button>
        </HStack>
        <HStack spacing={2}>
          <Text fontSize="sm" fontWeight="semibold" color="gray.600">Remote:</Text>
          <Button
            size="sm"
            leftIcon={<FiPause />}
            onClick={() => handleBulkPause(false, true)}
            colorScheme="orange"
            variant="outline"
          >
            Pause All
          </Button>
          <Button
            size="sm"
            leftIcon={<FiPlay />}
            onClick={() => handleBulkPause(false, false)}
            colorScheme="green"
            variant="outline"
          >
            Unpause All
          </Button>
        </HStack>
      </HStack>

      <VStack spacing={3} align="stretch" w="100%">
        <Box>
          {loading || error ? (
            <PageLoading loading={loading} error={error} />
          ) : (
            <DataTable
              data={data}
              columns={columns}
              searchPlaceholder="Search DAGs by ID, tags, owners..."
            />
          )}
        </Box>
      </VStack>
    </Box>
  );
}
