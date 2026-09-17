import { useEffect, useRef, useState, useCallback, useMemo } from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import {
  AlertDialog,
  AlertDialogBody,
  AlertDialogContent,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogOverlay,
  Badge,
  Box,
  Button,
  FormControl,
  Heading,
  HStack,
  Icon,
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
  Progress,
  Select,
  Stack,
  Switch,
  Tag,
  Text,
  Tooltip,
  useDisclosure,
  useToast,
  VStack,
} from '@chakra-ui/react';
import axios from 'axios';
import humanFormat from 'human-format';
import {
  ChevronLeftIcon,
  ChevronRightIcon,
  CloseIcon,
  ExternalLinkIcon,
  RepeatIcon,
  SearchIcon,
} from '@chakra-ui/icons';
import { FiFilter, FiPause, FiPlay } from 'react-icons/fi';
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
  const { limit, batchSize, page, pageSize, search, searchField } = useDagHistoryConfig();
  const dispatch = useAppDispatch();
  const toast = useToast();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState([]);
  const [totalCount, setTotalCount] = useState(0);
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
      if (searchField && searchField !== 'any') params.search_field = searchField;

      const [localRes, remoteRes] = await Promise.all([
        axios.get(localRoute(constants.DAGS_ROUTE), { params }),
        axios.get(proxyUrl(targetUrl + constants.DAGS_ROUTE), { params, headers: proxyHeaders(token) }),
      ]);

      if (localRes.status === 200 && remoteRes.status === 200) {
        // Response shape (>=2.11): {dags: [...], total_dag_count: N}. Older releases returned a bare list.
        const unwrap = (res) => (Array.isArray(res.data) ? res.data : (res.data?.dags ?? []));
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
  }, [targetUrl, token, dispatch, localAirflowVersion, page, pageSize, search, searchField]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // Keep local input mirror in sync when `search` changes externally
  // (e.g. token invalidation resetting AppContext).
  useEffect(() => {
    setSearchInput(search);
  }, [search]);

  // A saved page can point past totalCount (deployment shrank, filter narrowed,
  // or state was persisted from a larger deployment). Snap back to page 0.
  useEffect(() => {
    if (totalCount > 0 && page * pageSize >= totalCount) {
      dispatch({ type: 'set-dag-history-page', page: 0 });
    }
  }, [totalCount, page, pageSize, dispatch]);

  // Debounce search input -> AppContext dispatch (~300ms).
  useEffect(() => {
    if (searchInput === search) return undefined;
    const t = setTimeout(() => {
      dispatch({ type: 'set-dag-history-search', search: searchInput });
    }, 300);
    return () => clearTimeout(t);
  }, [searchInput, search, dispatch]);

  const patchPauseState = useCallback(
    async (isPaused, dagId, isLocal) => {
      const url = isLocal ? localRoute(constants.DAGS_ROUTE) : proxyUrl(targetUrl + constants.DAGS_ROUTE);
      const cfg = isLocal ? {} : { headers: proxyHeaders(token) };
      const res = await axios.patch(url, { dag_id: dagId, is_paused: isPaused }, cfg);
      return res.data;
    },
    [targetUrl, token],
  );

  const handlePausedClick = useCallback(
    async (isPaused, dagId, isLocal) => {
      try {
        const data = await patchPauseState(isPaused, dagId, isLocal);
        setData((prev) =>
          prev.map((item) => {
            if (item.local.dag_id !== dagId) return item;
            const key = isLocal ? 'local' : 'remote';
            return { ...item, [key]: { ...item[key], is_paused: data.is_paused } };
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
    [patchPauseState, toast],
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

  // Bulk pause/unpause: opens a confirmation modal, then fetches ALL matching
  // DAGs (across pages) and updates them. Respects the current search filter.
  const { isOpen: isBulkPauseOpen, onOpen: openBulkPauseDialog, onClose: closeBulkPauseDialog } = useDisclosure();
  const [bulkPauseIntent, setBulkPauseIntent] = useState(null);
  const [isBulkPausing, setIsBulkPausing] = useState(false);
  // Live progress shown inside the modal while the loop runs and after it finishes.
  const [bulkPauseProgress, setBulkPauseProgress] = useState(null);
  // Toggled by the Stop button to short-circuit the loop mid-run.
  const bulkPauseAbortRef = useRef(false);
  const bulkPauseCancelRef = useRef(null);

  const requestBulkPause = useCallback(
    (isLocal, pause) => {
      setBulkPauseIntent({ isLocal, pause });
      setBulkPauseProgress(null);
      bulkPauseAbortRef.current = false;
      openBulkPauseDialog();
    },
    [openBulkPauseDialog],
  );

  const dismissBulkPauseDialog = useCallback(() => {
    bulkPauseAbortRef.current = true;
    closeBulkPauseDialog();
    setBulkPauseIntent(null);
    setBulkPauseProgress(null);
  }, [closeBulkPauseDialog]);

  const runBulkPause = useCallback(async () => {
    if (!bulkPauseIntent) return;
    const { isLocal, pause } = bulkPauseIntent;
    setIsBulkPausing(true);
    bulkPauseAbortRef.current = false;

    try {
      const params = {};
      if (search) params.search = search;
      if (searchField && searchField !== 'any') params.search_field = searchField;

      const url = isLocal ? localRoute(constants.DAGS_ROUTE) : proxyUrl(targetUrl + constants.DAGS_ROUTE);
      const cfg = isLocal ? { params } : { params, headers: proxyHeaders(token) };
      const res = await axios.get(url, cfg);
      const dags = Array.isArray(res.data) ? res.data : (res.data?.dags ?? []);
      const targets = dags.filter((d) => d.is_paused !== pause);

      if (targets.length === 0) {
        setBulkPauseProgress({ done: 0, failed: 0, total: 0, finished: true, aborted: false });
        return;
      }

      setBulkPauseProgress({ done: 0, failed: 0, total: targets.length, finished: false, aborted: false });

      let done = 0;
      let failed = 0;
      // Batch state updates so a 1000-item loop doesn't cause 1000 re-renders;
      // always emit the final state so the modal shows the exact totals.
      const shouldEmit = (processed) => processed === targets.length || targets.length <= 50 || processed % 10 === 0;
      for (const dag of targets) {
        if (bulkPauseAbortRef.current) break;
        try {
          await patchPauseState(pause, dag.dag_id, isLocal);
          done += 1;
        } catch (_err) {
          failed += 1;
        }
        const processed = done + failed;
        if (shouldEmit(processed)) {
          setBulkPauseProgress({
            done,
            failed,
            total: targets.length,
            finished: processed === targets.length,
            aborted: false,
          });
        }
      }

      await fetchData();

      const aborted = bulkPauseAbortRef.current;
      setBulkPauseProgress({
        done,
        failed,
        total: targets.length,
        finished: true,
        aborted,
      });
    } catch (err) {
      setBulkPauseProgress({
        done: 0,
        failed: 0,
        total: 0,
        finished: true,
        aborted: false,
        error: err.message,
      });
    } finally {
      setIsBulkPausing(false);
    }
  }, [bulkPauseIntent, search, searchField, targetUrl, token, patchPauseState, fetchData]);

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
            onClick={() => requestBulkPause(true, true)}
            colorScheme="orange"
            variant="outline"
          >
            Pause All
          </Button>
          <Button
            size="sm"
            leftIcon={<FiPlay />}
            onClick={() => requestBulkPause(true, false)}
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
            onClick={() => requestBulkPause(false, true)}
            colorScheme="orange"
            variant="outline"
          >
            Pause All
          </Button>
          <Button
            size="sm"
            leftIcon={<FiPlay />}
            onClick={() => requestBulkPause(false, false)}
            colorScheme="green"
            variant="outline"
          >
            Unpause All
          </Button>
        </HStack>
      </HStack>

      <HStack spacing={3} mb={3} align="center">
        <HStack spacing={0} border="1px solid" borderColor="gray.200" borderRadius="md" overflow="hidden" bg="white">
          <HStack
            px={3}
            h="8"
            bg="gray.50"
            spacing={2}
            borderRight="1px solid"
            borderColor="gray.200"
            alignItems="center"
            flexShrink={0}
            lineHeight="1"
          >
            <Icon as={FiFilter} boxSize={4} color="gray.500" display="block" />
            <Text fontSize="sm" fontWeight="medium" color="gray.700" lineHeight="1">
              Filter
            </Text>
          </HStack>
          <Select
            size="sm"
            w="32"
            value={searchField}
            onChange={(e) => dispatch({ type: 'set-dag-history-search-field', searchField: e.target.value })}
            border="0"
            borderRadius={0}
            _focus={{ boxShadow: 'none' }}
            aria-label="Filter field"
          >
            <option value="any">Any field</option>
            <option value="dag_id">DAG ID</option>
            <option value="owner">Owner</option>
            <option value="tag">Tag</option>
          </Select>
          <InputGroup size="sm" w="64" borderLeft="1px solid" borderColor="gray.200">
            <InputLeftElement pointerEvents="none">
              <SearchIcon color="gray.400" boxSize={3.5} />
            </InputLeftElement>
            <Input
              border="0"
              borderRadius={0}
              placeholder={
                searchField === 'dag_id'
                  ? 'Search by DAG ID...'
                  : searchField === 'owner'
                    ? 'Search by owner...'
                    : searchField === 'tag'
                      ? 'Search by tag...'
                      : 'Search by ID, tag, or owner...'
              }
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
              _focus={{ boxShadow: 'none' }}
              aria-label="Filter value"
            />
          </InputGroup>
        </HStack>
        {(searchInput || (searchField && searchField !== 'any')) && (
          <Button
            size="sm"
            variant="ghost"
            leftIcon={<CloseIcon boxSize={2.5} />}
            onClick={() => {
              setSearchInput('');
              dispatch({ type: 'set-dag-history-search', search: '' });
              dispatch({ type: 'set-dag-history-search-field', searchField: 'any' });
            }}
          >
            Reset
          </Button>
        )}
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
            <DataTable data={data} columns={columns} showSearch={false} />
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
              onChange={(e) => dispatch({ type: 'set-dag-history-page-size', pageSize: Number(e.target.value) })}
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

      <AlertDialog
        isOpen={isBulkPauseOpen}
        leastDestructiveRef={bulkPauseCancelRef}
        onClose={dismissBulkPauseDialog}
        isCentered
        closeOnEsc={!isBulkPausing}
        closeOnOverlayClick={!isBulkPausing}
      >
        <AlertDialogOverlay>
          <AlertDialogContent>
            <AlertDialogHeader fontSize="lg" fontWeight="bold">
              {bulkPauseIntent?.pause ? 'Pause' : 'Unpause'} {bulkPauseIntent?.isLocal ? 'local' : 'remote'} DAGs
            </AlertDialogHeader>
            <AlertDialogBody>
              {!bulkPauseProgress ? (
                <>
                  This will {bulkPauseIntent?.pause ? 'pause' : 'unpause'}{' '}
                  {bulkPauseIntent?.isLocal ? (
                    <>
                      up to <strong>{totalCount}</strong> local DAG
                      {totalCount === 1 ? '' : 's'}
                    </>
                  ) : (
                    <>all matching remote DAGs</>
                  )}
                  {search ? (
                    <>
                      {' '}
                      matching search <em>&quot;{search}&quot;</em>
                      {searchField && searchField !== 'any' ? <> in {searchField.replace('_', ' ')}</> : null}
                    </>
                  ) : null}
                  . DAGs already in the desired state are skipped. Continue?
                </>
              ) : bulkPauseProgress.error ? (
                <Text color="red.600">Failed to fetch DAGs: {bulkPauseProgress.error}</Text>
              ) : bulkPauseProgress.total === 0 ? (
                <Text>
                  All matching {bulkPauseIntent?.isLocal ? 'local' : 'remote'} DAGs are already{' '}
                  {bulkPauseIntent?.pause ? 'paused' : 'active'}. Nothing to do.
                </Text>
              ) : (
                <Box>
                  <Text mb={2}>
                    {bulkPauseProgress.finished
                      ? bulkPauseProgress.aborted
                        ? 'Stopped: '
                        : 'Done: '
                      : bulkPauseIntent?.pause
                        ? 'Pausing '
                        : 'Unpausing '}
                    <strong>{bulkPauseProgress.done}</strong> of <strong>{bulkPauseProgress.total}</strong> DAGs
                    {bulkPauseProgress.failed > 0 ? (
                      <>
                        {' '}
                        (
                        <Text as="span" color="red.600">
                          {bulkPauseProgress.failed} failed
                        </Text>
                        )
                      </>
                    ) : null}
                  </Text>
                  <Progress
                    value={((bulkPauseProgress.done + bulkPauseProgress.failed) * 100) / bulkPauseProgress.total}
                    size="sm"
                    colorScheme={bulkPauseIntent?.pause ? 'orange' : 'green'}
                    hasStripe={!bulkPauseProgress.finished}
                    isAnimated={!bulkPauseProgress.finished}
                    borderRadius="md"
                  />
                </Box>
              )}
            </AlertDialogBody>
            <AlertDialogFooter>
              {!bulkPauseProgress ? (
                <>
                  <Button ref={bulkPauseCancelRef} onClick={dismissBulkPauseDialog} isDisabled={isBulkPausing}>
                    Cancel
                  </Button>
                  <Button
                    colorScheme={bulkPauseIntent?.pause ? 'orange' : 'green'}
                    onClick={runBulkPause}
                    ml={3}
                    isLoading={isBulkPausing}
                    loadingText="Loading DAGs..."
                  >
                    {bulkPauseIntent?.pause ? 'Pause All' : 'Unpause All'}
                  </Button>
                </>
              ) : !bulkPauseProgress.finished ? (
                <Button
                  ref={bulkPauseCancelRef}
                  colorScheme="red"
                  variant="outline"
                  onClick={() => {
                    bulkPauseAbortRef.current = true;
                  }}
                >
                  Stop
                </Button>
              ) : (
                <Button ref={bulkPauseCancelRef} onClick={dismissBulkPauseDialog}>
                  Close
                </Button>
              )}
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialogOverlay>
      </AlertDialog>
    </Box>
  );
}
