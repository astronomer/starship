/* eslint-disable no-nested-ternary */
import React, { useEffect, useState, useCallback } from 'react';
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
import PropTypes from 'prop-types';
import axios from 'axios';
import { MdErrorOutline, MdDeleteForever, MdWarning } from 'react-icons/md';
import { GoUpload } from 'react-icons/go';
import humanFormat from 'human-format';
import { ExternalLinkIcon, RepeatIcon } from '@chakra-ui/icons';
import { FiPause, FiPlay } from 'react-icons/fi';

import { useAppDispatch, useTargetConfig, useDagHistoryConfig } from '../AppContext';
import DataTable from '../component/DataTable';
import PageLoading from '../component/PageLoading';
import TooltipHeader from '../component/TooltipHeader';
import { localRoute, proxyHeaders, proxyUrl, getDagViewPath } from '../util';
import mergeDagData from '../utils/dagUtils';
import constants from '../constants';

const columnHelper = createColumnHelper();

// Helper component
function WithTooltip({ isDisabled = false, children }) {
  return isDisabled ? (
    <Tooltip hasArrow label={isDisabled}>{children}</Tooltip>
  ) : children;
}

WithTooltip.propTypes = {
  isDisabled: PropTypes.oneOfType([PropTypes.bool, PropTypes.string]),
  children: PropTypes.node.isRequired,
};


// Migrate button for DAG history
function DAGHistoryMigrateButton({
  url,
  token,
  dagId,
  limit = 1000,
  batchSize = 100,
  existsInRemote = false,
  isDisabled = false,
  onMigrate = null,
  onDelete = null,
}) {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const toast = useToast();
  const [exists, setExists] = useState(existsInRemote);
  const [isDeleting, setIsDeleting] = useState(false);

const handleError = (err) => {
    setExists(false);
    setIsLoading(false);
    setIsDeleting(false);
    toast({
      title: err.response?.data?.error || err.response?.data || err.message,
      status: 'error',
      isClosable: true,
      variant: 'outline',
      duration: 4000,
    });
    setError(err);
  };

  const handleClick = async () => {
    if (exists) {
      // Delete
      setIsDeleting(true);
      setIsLoading(true);
      try {
        const res = await axios({
          method: 'delete',
          url: proxyUrl(url + constants.DAG_RUNS_ROUTE),
          headers: proxyHeaders(token),
          params: { dag_id: dagId },
        });
        const newStatus = res.status !== 204;
        setExists(newStatus);
        onDelete?.(dagId);
        setTimeout(() => {
          setIsLoading(false);
          setIsDeleting(false);
        }, 500);
      } catch (err) {
        handleError(err);
        setIsDeleting(false);
      }
      return;
    }

    // Migrate
    setIsLoading(true);

    const migrateBatch = async (offset = 0) => {
      const appliedBatchSize = Math.min(limit - offset, batchSize);
      try {
        const [dagRunsRes, taskInstanceRes, taskInstanceHistoryRes] = await Promise.all([
          axios.get(localRoute(constants.DAG_RUNS_ROUTE), { params: { dag_id: dagId, limit: appliedBatchSize, offset } }),
          axios.get(localRoute(constants.TASK_INSTANCE_ROUTE), { params: { dag_id: dagId, limit: appliedBatchSize, offset } }),
          axios.get(localRoute(constants.TASK_INSTANCE_HISTORY_ROUTE), { params: { dag_id: dagId, limit: appliedBatchSize, offset } }),
        ]);

        const dagRunsToMigrateCount = Math.min(dagRunsRes.data.dag_run_count, limit);

        if (dagRunsRes.data.dag_runs.length === 0) {
          setExists(offset > 0);
          onMigrate?.(dagId, dagRunsToMigrateCount);
          setTimeout(() => setIsLoading(false), 500);
          return;
        }

        const dagRunCreateRes = await axios.post(
          proxyUrl(url + constants.DAG_RUNS_ROUTE),
          { dag_runs: dagRunsRes.data.dag_runs },
          { params: { dag_id: dagId }, headers: proxyHeaders(token) },
        );

        if (dagRunCreateRes.status !== 200) {
          throw new Error('Failed to create DAG runs');
        }

        await Promise.all([
          axios.post(
            proxyUrl(url + constants.TASK_INSTANCE_ROUTE),
            { task_instances: taskInstanceRes.data.task_instances },
            { params: { dag_id: dagId }, headers: proxyHeaders(token) },
          ),
          axios.post(
            proxyUrl(url + constants.TASK_INSTANCE_HISTORY_ROUTE),
            { task_instances: taskInstanceHistoryRes.data.task_instances },
            { params: { dag_id: dagId }, headers: proxyHeaders(token) },
          ),
        ]);

        if (dagRunCreateRes.data.dag_run_count < dagRunsToMigrateCount) {
          await migrateBatch(offset + batchSize);
        } else {
          setExists(true);
          onMigrate?.(dagId, dagRunsToMigrateCount);
          setTimeout(() => setIsLoading(false), 500);
        }
      } catch (err) {
        handleError(err);
      }
    };

    await migrateBatch(0);
  };

  return (
    <WithTooltip isDisabled={isDisabled}>
        <Button
          size="sm"
          variant="outline"
          isDisabled={isDisabled}
        isLoading={isLoading}
        loadingText={isDeleting ? 'Deleting...' : 'Migrating...'}
        leftIcon={
          error ? <MdErrorOutline />
            : exists ? <MdDeleteForever />
              : isDisabled ? <MdWarning />
                  : <GoUpload />
        }
        colorScheme={undefined}
        color={exists || error ? 'error.600' : 'success.600'}
        borderColor={exists || error ? 'error.500' : 'success.500'}
        _hover={{
          bg: exists || error ? 'error.50' : 'success.50',
        }}
        _disabled={{
          color: 'gray.600',
          borderColor: 'gray.500',
          opacity: 1,
        }}
        onClick={handleClick}
      >
        {exists ? 'Delete'
            : isDisabled ? 'Not Found'
              : error ? 'Error!'
                : 'Migrate'}
      </Button>
    </WithTooltip>
  );
}

DAGHistoryMigrateButton.propTypes = {
  url: PropTypes.string.isRequired,
  token: PropTypes.string.isRequired,
  dagId: PropTypes.string.isRequired,
  limit: PropTypes.number,
  batchSize: PropTypes.number,
  existsInRemote: PropTypes.bool,
  isDisabled: PropTypes.oneOfType([PropTypes.bool, PropTypes.string]),
  onMigrate: PropTypes.func,
  onDelete: PropTypes.func,
};



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
    const url = isLocal ? localRoute(constants.DAGS_ROUTE) : proxyUrl(targetUrl + constants.DAGS_ROUTE);
    try {
      const res = await axios.patch(url, { dag_id: dagId, is_paused: isPaused }, { headers: proxyHeaders(token) });
      setData((prev) => prev.map((item) => {
        if (item.local.dag_id !== dagId) return item;
        const key = isLocal ? 'local' : 'remote';
        return { ...item, [key]: { ...item[key], is_paused: res.data.is_paused } };
      }));
    } catch (err) {
      toast({ title: err.message, status: 'error', isClosable: true, variant: 'outline', duration: 4000 });
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
      toast({ title: `All ${isLocal ? 'local' : 'remote'} DAGs are already ${pause ? 'paused' : 'unpaused'}`, status: 'info', duration: 4000, variant: 'outline' });
      return;
    }

    let successCount = 0;
    for (const item of items) {
      try {
        await handlePausedClick(pause, item.local.dag_id, isLocal);
        successCount += 1;
      } catch (err) {
        // Continue on error
      }
    }

    toast({
      title: `${pause ? 'Paused' : 'Unpaused'} ${successCount} ${isLocal ? 'local' : 'remote'} DAG${successCount !== 1 ? 's' : ''}`,
      status: 'success',
      duration: 4000,
      variant: 'outline',
    });
  }, [data, toast, handlePausedClick]);

  const columns = React.useMemo(() => [
    columnHelper.accessor((row) => row.local.dag_id, {
      id: 'dagId',
      header: 'ID',
      cell: ({ row, getValue }) => (
        <Tooltip hasArrow label={`File: ${row.original.local.fileloc}`}>
          <Text fontWeight="semibold">
            {getValue()}
          </Text>
        </Tooltip>
      ),
    }),
    columnHelper.accessor((row) => row.local.tags, {
      id: 'tags',
      header: 'Tags',
      cell: ({ getValue }) => {
        const tags = getValue();
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
      },
    }),
    columnHelper.accessor((row) => row.local.schedule_interval, {
      id: 'schedule',
      header: 'Schedule',
      cell: ({ getValue }) => getValue() || 'None',
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
      header: () => <>Local <TooltipHeader tooltip="Toggle to pause/unpause DAG in local" /></>,
      cell: ({ row }) => (
        <>
          <Switch
            colorScheme="success"
            isChecked={!row.original.local.is_paused}
            onChange={() => handlePausedClick(!row.original.local.is_paused, row.original.local.dag_id, true)}
          />
          <Tooltip hasArrow label="DAG Run Count">
            <Badge mx={1} fontSize="sm" variant="outline" colorScheme={row.original.local.dag_run_count > 0 ? 'teal' : 'red'}>
              {humanFormat(row.original.local.dag_run_count)}
            </Badge>
          </Tooltip>
        </>
      ),
    }),
    columnHelper.display({
      id: 'local_url',
      header: 'Local URL',
      cell: ({ row }) => (
        <Link isExternal href={localRoute(getDagViewPath(row.original.local.dag_id, localAirflowVersion))} color="brand.700">
          View DAG <ExternalLinkIcon mx="2px" />
        </Link>
      ),
    }),
    columnHelper.display({
      id: 'remote_is_paused',
      header: () => <>Remote <TooltipHeader tooltip="Toggle to pause/unpause DAG in remote" /></>,
      cell: ({ row }) => row.original.remote ? (
        <>
          <Switch
            colorScheme="success"
            isChecked={!row.original.remote?.is_paused}
            onChange={() => handlePausedClick(!row.original.remote?.is_paused, row.original.local.dag_id, false)}
          />
          <Tooltip hasArrow label="DAG Run Count">
            <Badge mx={1} fontSize="sm" variant="outline" colorScheme={row.original.remote?.dag_run_count > 0 ? 'teal' : 'red'}>
              {humanFormat(row.original.remote?.dag_run_count || 0)}
            </Badge>
          </Tooltip>
        </>
      ) : null,
    }),
    columnHelper.display({
      id: 'remote_url',
      header: 'Remote URL',
      cell: ({ row }) => row.original.remote ? (
        <Link isExternal href={`${targetUrl}/dags/${row.original.remote.dag_id}`} color="brand.700">
          View DAG <ExternalLinkIcon mx="2px" />
        </Link>
      ) : null,
    }),
    columnHelper.display({
      id: 'migrate',
      header: 'Migrate',
      meta: { align: 'right' },
      cell: ({ row }) => (
        <DAGHistoryMigrateButton
          url={targetUrl}
          token={token}
          dagId={row.original.local.dag_id}
          limit={Number(limit)}
          batchSize={Number(batchSize)}
          existsInRemote={!!row.original.remote?.dag_run_count}
          isDisabled={
            !row.original.remote?.dag_id ? 'DAG not found in remote'
              : !row.original.local.dag_run_count ? 'No DAG Runs to migrate'
                : false
          }
          onMigrate={handleMigrate}
          onDelete={handleDelete}
        />
      ),
    }),
  ], [targetUrl, token, limit, batchSize, handleMigrate, handleDelete, localAirflowVersion]);

  return (
    <Box>
      <Stack direction={{ base: 'column', md: 'row' }} justify="space-between" align={{ base: 'flex-start', md: 'center' }} mb={3}>
        <Box>
          <Heading size="md" mb={0.5}>DAG History</Heading>
          <Text fontSize="xs" color="gray.600">Migrate DAGs and task history to prevent rescheduling.</Text>
        </Box>
        <HStack spacing={2}>
          <Tooltip hasArrow label="Total DAG Runs to migrate">
            <FormControl minW="40">
              <InputGroup size="sm">
                <InputLeftAddon># DAG Runs</InputLeftAddon>
                <NumberInput value={limit} onChange={(val) => dispatch({ type: 'set-limit', limit: Number(val) })}>
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
                <NumberInput value={batchSize} onChange={(val) => dispatch({ type: 'set-batch-size', batchSize: Number(val) })}>
                  <NumberInputField />
                  <NumberInputStepper>
                    <NumberIncrementStepper />
                    <NumberDecrementStepper />
                  </NumberInputStepper>
                </NumberInput>
              </InputGroup>
            </FormControl>
          </Tooltip>
          <Button size="sm" leftIcon={<RepeatIcon />} onClick={fetchData} variant="outline" isLoading={loading} flexShrink={0}>
            Refresh
          </Button>
        </HStack>
      </Stack>

      <HStack spacing={2} mb={3} justify="space-between">
        <HStack spacing={2}>
          <Text fontSize="sm" fontWeight="semibold" color="gray.600">Local:</Text>
          <Button size="sm" leftIcon={<FiPause />} onClick={() => handleBulkPause(true, true)} colorScheme="orange" variant="outline" >
            Pause All
          </Button>
          <Button size="sm" leftIcon={<FiPlay />} onClick={() => handleBulkPause(true, false)} colorScheme="green" variant="outline">
            Unpause All
          </Button>
        </HStack>
        <HStack spacing={2}>
          <Text fontSize="sm" fontWeight="semibold" color="gray.600">Remote:</Text>
          <Button size="sm" leftIcon={<FiPause />} onClick={() => handleBulkPause(false, true)} colorScheme="orange" variant="outline" >
            Pause All
          </Button>
          <Button size="sm" leftIcon={<FiPlay />} onClick={() => handleBulkPause(false, false)} colorScheme="green" variant="outline" >
            Unpause All
          </Button>
        </HStack>
      </HStack>

      <VStack spacing={3} align="stretch" w="100%">
        <Box>
          {loading || error ? (
            <PageLoading loading={loading} error={error} />
          ) : (
            <DataTable data={data} columns={columns} searchPlaceholder="Search DAGs by ID, tags, owners..." />
          )}
        </Box>
      </VStack>
    </Box>
  );
}
