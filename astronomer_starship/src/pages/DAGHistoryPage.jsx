import { useEffect, useState, useCallback, useMemo } from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import {
  Badge,
  Box,
  Button,
  Checkbox,
  FormControl,
  Heading,
  HStack,
  IconButton,
  Input,
  InputGroup,
  InputLeftAddon,
  InputLeftElement,
  Link,
  NumberDecrementStepper,
  NumberIncrementStepper,
  NumberInput,
  NumberInputField,
  NumberInputStepper,
  Select,
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
import { ChevronLeftIcon, ChevronRightIcon, ExternalLinkIcon, RepeatIcon, SearchIcon } from '@chakra-ui/icons';
import { FiPause, FiPlay } from 'react-icons/fi';
import { useAppDispatch, useTargetConfig, useDagHistoryConfig } from '../AppContext';
import DataTable from '../component/DataTable';
import PageLoading from '../component/PageLoading';
import TooltipHeader from '../component/TooltipHeader';
import DAGHistoryMigrateButton, { BUTTON_STATES } from '../component/DAGHistoryMigrateButton';
import { localRoute, proxyHeaders, proxyUrl, getDagViewPath } from '../util';
import mergeDagData from '../utils/dagUtils';
import constants from '../constants';

const columnHelper = createColumnHelper();

// Cell renderer functions - defined outside component to avoid unstable nested components
function renderDagId(info) {
  const { row, getValue } = info;
  return (
    <Tooltip hasArrow label={`File: ${row.original.local.fileloc}`}>
      <Text fontWeight="semibold">{getValue()}</Text>
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
  const { targetUrl, token, limit, batchSize, handleMigrate, handleDelete, localAirflowVersion, handlePausedClick } =
    config;

  return [
    columnHelper.accessor((row) => row.local.dag_id, {
      id: 'dagId',
      header: 'ID',
      cell: renderDagId,
      meta: { minWidth: '150px' },
    }),
    columnHelper.accessor((row) => row.local.tags, {
      id: 'tags',
      header: 'Tags',
      cell: renderTags,
      meta: { minWidth: '120px' },
    }),
    columnHelper.accessor((row) => row.local.schedule_interval, {
      id: 'schedule',
      header: 'Schedule',
      cell: renderSchedule,
      meta: { minWidth: '100px' },
    }),
    columnHelper.accessor((row) => row.local.description, {
      id: 'description',
      header: 'Description',
      meta: { minWidth: '200px' },
    }),
    columnHelper.accessor((row) => row.local.owners, {
      id: 'owners',
      header: 'Owners',
      meta: { minWidth: '120px' },
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
      meta: { width: '100px' },
      cell: (info) => {
        const { original } = info.row;
        return (
          <>
            <Switch
              colorScheme="success"
              isChecked={!original.local.is_paused}
              onChange={() => handlePausedClick(!original.local.is_paused, original.local.dag_id, true)}
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
      meta: { width: '110px' },
      enableSorting: false,
      cell: (info) => {
        const { original } = info.row;
        return (
          <Link
            isExternal
            href={localRoute(getDagViewPath(original.local.dag_id, localAirflowVersion))}
            color="brand.700"
          >
            View DAG <ExternalLinkIcon mx="2px" />
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
      meta: { width: '100px' },
      cell: (info) => {
        const { original } = info.row;
        if (!original.remote) return null;
        return (
          <>
            <Switch
              colorScheme="success"
              isChecked={!original.remote?.is_paused}
              onChange={() => handlePausedClick(!original.remote?.is_paused, original.local.dag_id, false)}
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
      meta: { width: '110px' },
      enableSorting: false,
      cell: (info) => {
        const { original } = info.row;
        if (!original.remote) return null;
        return (
          <Link isExternal href={`${targetUrl}/dags/${original.remote.dag_id}`} color="brand.700">
            View DAG <ExternalLinkIcon mx="2px" />
          </Link>
        );
      },
    }),
    columnHelper.display({
      id: 'migrate',
      header: 'Migrate',
      meta: { align: 'right', width: '170px' },
      enableSorting: false,
      cell: (info) => {
        const { original } = info.row;

        let disabledReason = null;

        if (!original.remote?.dag_id) {
          disabledReason = BUTTON_STATES.NOT_IN_REMOTE;
        } else if (!original.local.dag_run_count && !original.remote?.dag_run_count) {
          disabledReason = BUTTON_STATES.NO_DAG_RUNS;
        }

        return (
          <DAGHistoryMigrateButton
            url={targetUrl}
            token={token}
            dagId={original.local.dag_id}
            limit={Number(limit)}
            batchSize={Number(batchSize)}
            existsInRemote={!!original.remote?.dag_run_count}
            disabledReason={disabledReason}
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
  const { limit, batchSize, page, pageSize, search, unmigratedOnly } = useDagHistoryConfig();
  const dispatch = useAppDispatch();
  const toast = useToast();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState([]);
  const [totalCount, setTotalCount] = useState(0);
  // Set of dag_ids that exist on the target -- used by the "unmigrated only" filter.
  // Fetched once per page mount; stays local (not in AppContext) since it can be sizeable.
  const [targetDagIds, setTargetDagIds] = useState(() => new Set());
  // Local mirror of the search input so we can debounce dispatches to AppContext.
  const [searchInput, setSearchInput] = useState(search);

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

      const params = { limit: pageSize, offset: page * pageSize };
      if (search) params.search = search;

      const [localRes, remoteRes] = await Promise.all([
        axios.get(localRoute(constants.DAGS_ROUTE), { params }),
        axios.get(proxyUrl(targetUrl + constants.DAGS_ROUTE), { params, headers: proxyHeaders(token) }),
      ]);

      if (localRes.status === 200 && remoteRes.status === 200) {
        // Response shape (>=2.11): {dags: [...], total_dag_count: N}. Older releases returned a bare list.
        const unwrap = (res) => (Array.isArray(res.data) ? res.data : res.data?.dags ?? []);
        const localDags = unwrap(localRes);
        const remoteDags = unwrap(remoteRes);
        setData(mergeDagData(localDags, remoteDags));
        // Total from the source is authoritative for pagination; target count may differ.
        const nextTotal = Array.isArray(localRes.data)
          ? localDags.length
          : (localRes.data?.total_dag_count ?? localDags.length);
        setTotalCount(nextTotal);
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
  }, [targetUrl, token, dispatch, localAirflowVersion, page, pageSize, search]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // One-time (per targetUrl change) fetch of every target dag_id so the "unmigrated only"
  // filter can flag migrated rows accurately across pages. Payload is dag_ids-only-ish
  // and typically <100KB even for thousands of DAGs.
  useEffect(() => {
    if (!targetUrl || !token) return undefined;
    let cancelled = false;
    (async () => {
      try {
        const res = await axios.get(proxyUrl(targetUrl + constants.DAGS_ROUTE), {
          params: { limit: 10000, offset: 0 },
          headers: proxyHeaders(token),
        });
        if (cancelled) return;
        const dags = Array.isArray(res.data) ? res.data : res.data?.dags ?? [];
        setTargetDagIds(new Set(dags.map((d) => d.dag_id)));
      } catch {
        // Non-fatal: the filter just becomes a no-op if we can't reach target here.
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [targetUrl, token]);

  // Debounce search input -> AppContext dispatch (~300ms).
  useEffect(() => {
    if (searchInput === search) return undefined;
    const t = setTimeout(() => {
      dispatch({ type: 'set-dag-history-search', search: searchInput });
    }, 300);
    return () => clearTimeout(t);
  }, [searchInput, search, dispatch]);

  const handlePausedClick = useCallback(
    async (isPaused, dagId, isLocal) => {
      const url = isLocal ? localRoute(constants.DAGS_ROUTE) : proxyUrl(targetUrl + constants.DAGS_ROUTE);
      try {
        const res = await axios.patch(url, { dag_id: dagId, is_paused: isPaused }, { headers: proxyHeaders(token) });
        setData((prev) =>
          prev.map((item) => {
            if (item.local.dag_id !== dagId) return item;
            const key = isLocal ? 'local' : 'remote';
            return { ...item, [key]: { ...item[key], is_paused: res.data.is_paused } };
          }),
        );
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
    },
    [targetUrl, token, toast],
  );

  const handleMigrate = useCallback((dagId, runCount) => {
    setData((prev) =>
      prev.map((item) => {
        if (item.local.dag_id !== dagId) return item;
        // Only update dag_run_count if remote exists, otherwise keep remote as null
        if (!item.remote) return item;
        return { ...item, remote: { ...item.remote, dag_run_count: runCount } };
      }),
    );
  }, []);

  const handleDelete = useCallback((dagId) => {
    setData((prev) =>
      prev.map((item) => {
        if (item.local.dag_id !== dagId) return item;
        // Only update dag_run_count if remote exists, otherwise keep remote as null
        if (!item.remote) return item;
        return { ...item, remote: { ...item.remote, dag_run_count: 0 } };
      }),
    );
  }, []);

  const handleBulkPause = useCallback(
    async (isLocal, pause) => {
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

      for (const item of items) {
        try {
          await handlePausedClick(pause, item.local.dag_id, isLocal);
          successCount += 1;
        } catch (_err) {
          // Continue on error
        }
      }

      const dagLabel = successCount !== 1 ? 'DAGs' : 'DAG';
      const locationLabel = isLocal ? 'local' : 'remote';
      const failedCount = items.length - successCount;
      toast({
        title: `${pause ? 'Paused' : 'Activated'} ${successCount} ${locationLabel} ${dagLabel}`,
        description:
          failedCount > 0
            ? `${failedCount} ${failedCount !== 1 ? 'DAGs' : 'DAG'} failed to update`
            : `Successfully updated ${locationLabel} Airflow instance`,
        status: failedCount > 0 ? 'warning' : 'success',
        duration: 4000,
        variant: 'outline',
      });
    },
    [data, toast, handlePausedClick],
  );

  const columns = useMemo(
    () =>
      createColumns({
        targetUrl,
        token,
        limit,
        batchSize,
        handleMigrate,
        handleDelete,
        localAirflowVersion,
        handlePausedClick,
      }),
    [targetUrl, token, limit, batchSize, handleMigrate, handleDelete, localAirflowVersion, handlePausedClick],
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
          <Heading size="md" mb={0.5}>
            DAG History
          </Heading>
          <Text fontSize="xs" color="gray.600">
            Migrate DAGs and task history to prevent rescheduling.
          </Text>
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
          <Text fontSize="sm" fontWeight="semibold" color="gray.600">
            Local:
          </Text>
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
          <Text fontSize="sm" fontWeight="semibold" color="gray.600">
            Remote:
          </Text>
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

      <HStack spacing={3} mb={3} align="center">
        <InputGroup size="sm" maxW="sm">
          <InputLeftElement pointerEvents="none">
            <SearchIcon color="gray.400" />
          </InputLeftElement>
          <Input
            placeholder="Search DAGs by ID, tag, owner..."
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
          />
        </InputGroup>
        <Checkbox
          isChecked={unmigratedOnly}
          onChange={(e) =>
            dispatch({ type: 'set-dag-history-unmigrated-only', unmigratedOnly: e.target.checked })
          }
        >
          Unmigrated only
        </Checkbox>
        <Text fontSize="sm" color="gray.600" ml="auto">
          {totalCount === 0
            ? 'No DAGs'
            : `Showing ${page * pageSize + 1}\u2013${Math.min((page + 1) * pageSize, totalCount)} of ${totalCount}`}
        </Text>
      </HStack>

      <VStack spacing={3} align="stretch" w="100%">
        <Box>
          {loading || error ? (
            <PageLoading loading={loading} error={error} />
          ) : (
            <DataTable
              data={unmigratedOnly ? data.filter((d) => !targetDagIds.has(d.local.dag_id)) : data}
              columns={columns}
              showSearch={false}
            />
          )}
        </Box>
        {!loading && !error && totalCount > 0 && (
          <HStack spacing={2} justify="flex-end" align="center">
            <Text fontSize="sm" color="gray.600">
              Rows per page:
            </Text>
            <Select
              size="sm"
              w="20"
              value={pageSize}
              onChange={(e) =>
                dispatch({ type: 'set-dag-history-page-size', pageSize: Number(e.target.value) })
              }
            >
              <option value={25}>25</option>
              <option value={50}>50</option>
              <option value={100}>100</option>
              <option value={200}>200</option>
            </Select>
            <IconButton
              size="sm"
              aria-label="Previous page"
              icon={<ChevronLeftIcon />}
              onClick={() => dispatch({ type: 'set-dag-history-page', page: Math.max(0, page - 1) })}
              isDisabled={page === 0}
            />
            <Text fontSize="sm" color="gray.600" minW="20" textAlign="center">
              Page {page + 1} of {Math.max(1, Math.ceil(totalCount / pageSize))}
            </Text>
            <IconButton
              size="sm"
              aria-label="Next page"
              icon={<ChevronRightIcon />}
              onClick={() => dispatch({ type: 'set-dag-history-page', page: page + 1 })}
              isDisabled={(page + 1) * pageSize >= totalCount}
            />
          </HStack>
        )}
      </VStack>
    </Box>
  );
}
