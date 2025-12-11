/* eslint-disable no-nested-ternary */
import React, { useEffect, useState, useCallback } from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import {
  Badge,
  Box,
  Button,
  CircularProgress,
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
import { MdErrorOutline, MdDeleteForever } from 'react-icons/md';
import { GrDocumentMissing } from 'react-icons/gr';
import { GoUpload } from 'react-icons/go';
import humanFormat from 'human-format';
import { ExternalLinkIcon, RepeatIcon } from '@chakra-ui/icons';
import { FiPause, FiPlay } from 'react-icons/fi';

import { useAppState, useAppDispatch } from '../AppContext';
import DataTable from '../component/DataTable';
import PageLoading from '../component/PageLoading';
import TooltipHeader from '../component/TooltipHeader';
import { localRoute, proxyHeaders, proxyUrl } from '../util';
import constants from '../constants';

const columnHelper = createColumnHelper();

// Helper component
function WithTooltip({ isDisabled, children }) {
  return isDisabled ? (
    <Tooltip hasArrow label={isDisabled}>{children}</Tooltip>
  ) : children;
}

WithTooltip.propTypes = {
  isDisabled: PropTypes.oneOfType([PropTypes.bool, PropTypes.string]),
  children: PropTypes.node.isRequired,
};

WithTooltip.defaultProps = {
  isDisabled: false,
};

// Migrate button for DAG history
function DAGHistoryMigrateButton({
  url, token, dagId, limit, batchSize, existsInRemote, isDisabled, onMigrate, onDelete,
}) {
  const [loadPerc, setLoadPerc] = useState(0);
  const [error, setError] = useState(null);
  const toast = useToast();
  const [exists, setExists] = useState(existsInRemote);

const handleError = (err) => {
    setExists(false);
    setLoadPerc(0);
    toast({
      title: err.response?.data?.error || err.response?.data || err.message,
      status: 'error',
      isClosable: true,
    });
    setError(err);
  };

  const handleClick = async () => {
    if (exists) {
      // Delete
      setLoadPerc(50);
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
        setLoadPerc(100);
        setTimeout(() => setLoadPerc(0), 500);
      } catch (err) {
        handleError(err);
      }
      return;
    }

    // Migrate
    setLoadPerc(1);

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
          setLoadPerc(100);
          setExists(offset > 0);
          onMigrate?.(dagId, dagRunsToMigrateCount);
          setTimeout(() => setLoadPerc(0), 500);
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
          setLoadPerc((100 * dagRunCreateRes.data.dag_run_count) / dagRunsToMigrateCount);
          await migrateBatch(offset + batchSize);
        } else {
          setLoadPerc(100);
          setExists(true);
          onMigrate?.(dagId, dagRunsToMigrateCount);
          setTimeout(() => setLoadPerc(0), 500);
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
        isDisabled={isDisabled || loadPerc > 0}
        leftIcon={
          error ? <MdErrorOutline />
            : exists ? <MdDeleteForever />
              : isDisabled ? <GrDocumentMissing />
                : loadPerc ? <span />
                  : <GoUpload />
        }
        colorScheme={exists ? 'red' : isDisabled ? 'gray' : error ? 'red' : 'green'}
        onClick={handleClick}
      >
        {exists ? 'Delete'
          : loadPerc ? <CircularProgress thickness="20px" size="30px" value={loadPerc} />
            : isDisabled ? ''
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

DAGHistoryMigrateButton.defaultProps = {
  limit: 1000,
  batchSize: 100,
  existsInRemote: false,
  isDisabled: false,
  onMigrate: null,
  onDelete: null,
};

// Merge local and remote DAG data
function mergeDagData(localData, remoteData) {
  const output = {};
  localData.forEach((item) => {
    output[item.dag_id] = { local: item, remote: null };
  });
  remoteData.forEach((item) => {
    if (output[item.dag_id]) {
      output[item.dag_id].remote = item;
    }
  });
  return Object.values(output);
}

export default function DAGHistoryPage() {
  const { targetUrl, token, limit, batchSize } = useAppState();
  const dispatch = useAppDispatch();
  const toast = useToast();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState([]);

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
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
  }, [targetUrl, token, dispatch]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handlePausedClick = async (isPaused, dagId, isLocal) => {
    const url = isLocal ? localRoute(constants.DAGS_ROUTE) : proxyUrl(targetUrl + constants.DAGS_ROUTE);
    try {
      const res = await axios.patch(url, { dag_id: dagId, is_paused: isPaused }, { headers: proxyHeaders(token) });
      setData((prev) => prev.map((item) => {
        if (item.local.dag_id !== dagId) return item;
        const key = isLocal ? 'local' : 'remote';
        return { ...item, [key]: { ...item[key], is_paused: res.data.is_paused } };
      }));
    } catch (err) {
      toast({ title: err.message, status: 'error', isClosable: true });
    }
  };

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

  const handleBulkPause = async (isLocal, pause) => {
    const items = data.filter((item) => {
      const target = isLocal ? item.local : item.remote;
      return target && target.is_paused !== pause;
    });

    if (items.length === 0) {
      toast({ title: `All ${isLocal ? 'source' : 'destination'} DAGs are already ${pause ? 'paused' : 'unpaused'}`, status: 'info', duration: 3000 });
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
      title: `${pause ? 'Paused' : 'Unpaused'} ${successCount} ${isLocal ? 'source' : 'destination'} DAG${successCount !== 1 ? 's' : ''}`,
      status: 'success',
      duration: 3000,
    });
  };

  const columns = React.useMemo(() => [
    columnHelper.accessor((row) => row.local.dag_id, {
      id: 'dagId',
      header: 'ID',
      cell: ({ row, getValue }) => (
        <Tooltip hasArrow label={`File: ${row.original.local.fileloc}`}>
          <Link isExternal href={localRoute(`/dags/${getValue()}`)} color="moonshot.700" fontWeight="semibold">
            {getValue()}<ExternalLinkIcon mx="2px" />
          </Link>
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
              <Tag key={tag} size="sm" variant="ghost" colorScheme="brand">
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
      cell: ({ getValue }) => <Tag>{getValue() || 'None'}</Tag>,
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
      header: () => <>Local <TooltipHeader tooltip="Toggle to pause/unpause DAG in source" /></>,
      cell: ({ row }) => (
        <>
          <Tooltip hasArrow label="DAG Run Count">
            <Badge mx={1} fontSize="sm" variant="outline" colorScheme={row.original.local.dag_run_count > 0 ? 'teal' : 'red'}>
              {humanFormat(row.original.local.dag_run_count)}
            </Badge>
          </Tooltip>
          <Switch
            colorScheme="success"
            isChecked={!row.original.local.is_paused}
            onChange={() => handlePausedClick(!row.original.local.is_paused, row.original.local.dag_id, true)}
          />
        </>
      ),
    }),
    columnHelper.display({
      id: 'remote_is_paused',
      header: () => <>Remote <TooltipHeader tooltip="Toggle to pause/unpause DAG in destination" /></>,
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
          <Link isExternal href={`${targetUrl}/dags/${row.original.remote.dag_id}`} mx="2px">
            (<ExternalLinkIcon mx="2px" />)
          </Link>
        </>
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
  ], [targetUrl, token, limit, batchSize, handleMigrate, handleDelete]);

  return (
    <Box>
      <Stack direction={{ base: 'column', md: 'row' }} justify="space-between" align={{ base: 'flex-start', md: 'center' }} mb={3}>
        <Box>
          <Heading size="md" mb={0.5}>DAG History</Heading>
          <Text fontSize="xs" color="gray.600">Migrate DAGs and task history to prevent rescheduling.</Text>
        </Box>
        <HStack spacing={2}>
          <Tooltip hasArrow label="Total DAG Runs to migrate">
            <FormControl minWidth="150px">
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
            <FormControl minWidth="150px">
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
          <Text fontSize="sm" fontWeight="semibold" color="gray.600">Source:</Text>
          <Button size="sm" leftIcon={<FiPause />} onClick={() => handleBulkPause(true, true)} variant="outline" colorScheme="orange">
            Pause All
          </Button>
          <Button size="sm" leftIcon={<FiPlay />} onClick={() => handleBulkPause(true, false)} variant="outline" colorScheme="teal">
            Unpause All
          </Button>
        </HStack>
        <HStack spacing={2}>
          <Text fontSize="sm" fontWeight="semibold" color="gray.600">Destination:</Text>
          <Button size="sm" leftIcon={<FiPause />} onClick={() => handleBulkPause(false, true)} variant="outline" colorScheme="orange">
            Pause All
          </Button>
          <Button size="sm" leftIcon={<FiPlay />} onClick={() => handleBulkPause(false, false)} variant="outline" colorScheme="green">
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
