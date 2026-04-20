import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  AlertIcon,
  Badge,
  Box,
  Button,
  Card,
  CardBody,
  Code,
  HStack,
  Heading,
  IconButton,
  Link,
  Menu,
  MenuButton,
  MenuDivider,
  MenuItem,
  MenuList,
  Progress,
  Spinner,
  Stack,
  Stat,
  StatLabel,
  StatNumber,
  Table,
  TableContainer,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tooltip,
  Tr,
  VStack,
  useToast,
} from '@chakra-ui/react';
import { ArrowBackIcon, ChevronDownIcon, RepeatIcon } from '@chakra-ui/icons';
import axios from 'axios';
import { NavLink, useParams } from 'react-router-dom';
import constants, { ROUTES } from '../constants';
import { localRoute } from '../util';
import ConfirmDialog from '../component/ConfirmDialog';
import useConfirm from '../hooks/useConfirm';

const STATUS_COLORS = {
  running: 'info',
  completed: 'success',
  failed: 'error',
  aborted: 'warning',
  rolled_back: 'warning',
  deferred: 'amethyst',
  skipped: 'gray',
  pending: 'gray',
};

// DAG statuses that allow each action. Kept in one place so the action bar
// and the per-DAG menu stay consistent.
const CAN_ROLLBACK_DAG = new Set(['completed', 'failed']);
const CAN_RETRY_DAG = new Set(['failed', 'skipped']);
const CAN_PURGE_DAG = new Set(['completed', 'failed', 'skipped', 'aborted', 'rolled_back']);

function formatTimestamp(iso) {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}

function extractError(err) {
  const raw = err.response?.data?.error || err.message || 'Unknown error';
  return typeof raw === 'string' ? raw : JSON.stringify(raw);
}

function ProgressBar({ summary }) {
  const total = summary?.total || 0;
  if (!total) return null;
  const done = (summary.completed || 0) + (summary.failed || 0) + (summary.rolled_back || 0);
  const pct = Math.round((done / total) * 100);
  return (
    <Box>
      <HStack justify="space-between" mb={1}>
        <Text fontSize="xs" color="gray.600">
          {done} of {total} DAGs terminal ({pct}%)
        </Text>
      </HStack>
      <Progress value={pct} size="xs" colorScheme="brand" borderRadius="sm" />
    </Box>
  );
}

function SummaryGrid({ summary }) {
  if (!summary) return null;
  const cells = [
    { label: 'Total', value: summary.total, color: 'gray.700' },
    { label: 'Completed', value: summary.completed, color: 'success.500' },
    { label: 'Running', value: summary.running, color: 'info.500' },
    { label: 'Pending', value: summary.pending, color: 'gray.500' },
    { label: 'Failed', value: summary.failed, color: 'error.500' },
    { label: 'Rolled back', value: summary.rolled_back, color: 'warning.500' },
    { label: 'Deferred', value: summary.deferred, color: 'amethyst.500' },
    { label: 'Aborted', value: summary.aborted, color: 'warning.600' },
    { label: 'Skipped', value: summary.skipped, color: 'gray.400' },
  ];
  return (
    <HStack spacing={6} wrap="wrap">
      {cells.map((c) => (
        <Stat key={c.label} size="sm">
          <StatLabel fontSize="xs" color={c.color}>
            {c.label}
          </StatLabel>
          <StatNumber fontSize="lg" color={c.color}>
            {c.value ?? 0}
          </StatNumber>
        </Stat>
      ))}
    </HStack>
  );
}

// ---------------------------------------------------------------------------
// Wave-level action bar
// ---------------------------------------------------------------------------

function WaveActions({ wave, onAction }) {
  const summary = wave.summary || {};
  const isRunning = wave.status === 'running';
  const hasFailed = (summary.failed || 0) > 0;
  const hasSkipped = (summary.skipped || 0) > 0;
  // A wave is rollbackable when at least one DAG has reached a terminal
  // state that has migrated data to undo.
  const hasTerminal = (summary.completed || 0) + (summary.failed || 0) > 0;

  return (
    <Stack direction={{ base: 'column', md: 'row' }} spacing={2} wrap="wrap">
      <Button
        size="sm"
        colorScheme="warning"
        variant="outline"
        isDisabled={!isRunning}
        onClick={() => onAction('abort')}
      >
        Abort wave
      </Button>
      <Button
        size="sm"
        colorScheme="amethyst"
        variant="outline"
        isDisabled={!hasFailed || isRunning}
        onClick={() => onAction('retry-failed')}
      >
        Retry failed{hasFailed ? ` (${summary.failed})` : ''}
      </Button>
      <Button
        size="sm"
        colorScheme="amethyst"
        variant="outline"
        isDisabled={!hasSkipped || isRunning}
        onClick={() => onAction('retry-skipped')}
      >
        Retry skipped{hasSkipped ? ` (${summary.skipped})` : ''}
      </Button>
      <Button
        size="sm"
        colorScheme="red"
        variant="outline"
        isDisabled={!hasTerminal || isRunning}
        onClick={() => onAction('rollback-wave')}
      >
        Rollback wave
      </Button>
      <Button
        size="sm"
        colorScheme="red"
        variant="outline"
        isDisabled={isRunning}
        onClick={() => onAction('purge-wave')}
      >
        Purge wave metadata
      </Button>
    </Stack>
  );
}

// ---------------------------------------------------------------------------
// Per-DAG table with per-row actions
// ---------------------------------------------------------------------------

function DagTable({ dags, isWaveRunning, onDagAction }) {
  const rows = useMemo(
    () =>
      Object.entries(dags || {})
        .map(([dagId, state]) => ({ dagId, ...state }))
        .sort((a, b) => a.dagId.localeCompare(b.dagId)),
    [dags],
  );

  if (!rows.length) {
    return (
      <Text fontSize="sm" color="gray.500" fontStyle="italic">
        No DAGs in this wave.
      </Text>
    );
  }

  return (
    <TableContainer>
      <Table size="sm" variant="striped">
        <Thead>
          <Tr>
            <Th>DAG</Th>
            <Th>Status</Th>
            <Th>Step</Th>
            <Th isNumeric>Runs</Th>
            <Th isNumeric>TIs</Th>
            <Th>Latest interval end</Th>
            <Th>Error</Th>
            <Th textAlign="right">Actions</Th>
          </Tr>
        </Thead>
        <Tbody>
          {rows.map((r) => {
            const canRollback = !isWaveRunning && CAN_ROLLBACK_DAG.has(r.status);
            const canRetry = !isWaveRunning && CAN_RETRY_DAG.has(r.status);
            const canPurge = !isWaveRunning && CAN_PURGE_DAG.has(r.status);
            const anyEnabled = canRollback || canRetry || canPurge;
            return (
              <Tr key={r.dagId}>
                <Td fontFamily="mono" fontSize="xs">
                  {r.dagId}
                </Td>
                <Td>
                  <Badge colorScheme={STATUS_COLORS[r.status] || 'gray'}>{r.status}</Badge>
                </Td>
                <Td fontSize="xs">{r.step || '—'}</Td>
                <Td isNumeric fontSize="xs">
                  {r.dag_runs_migrated ?? 0}
                </Td>
                <Td isNumeric fontSize="xs">
                  {r.task_instances_migrated ?? 0}
                </Td>
                <Td fontSize="xs">{formatTimestamp(r.latest_data_interval_end)}</Td>
                <Td fontSize="xs" color="error.600" maxW="48" whiteSpace="normal" wordBreak="break-word">
                  {r.error || ''}
                </Td>
                <Td textAlign="right">
                  <Menu placement="bottom-end">
                    <MenuButton
                      as={Button}
                      size="xs"
                      variant="ghost"
                      rightIcon={<ChevronDownIcon />}
                      isDisabled={!anyEnabled}
                    >
                      Actions
                    </MenuButton>
                    <MenuList fontSize="sm">
                      <MenuItem isDisabled={!canRetry} onClick={() => onDagAction('retry', r.dagId)}>
                        Retry this DAG
                      </MenuItem>
                      <MenuItem isDisabled={!canRollback} onClick={() => onDagAction('rollback', r.dagId)}>
                        Rollback this DAG
                      </MenuItem>
                      <MenuDivider />
                      <MenuItem
                        color="error.600"
                        isDisabled={!canPurge}
                        onClick={() => onDagAction('purge', r.dagId)}
                      >
                        Purge this DAG&apos;s metadata
                      </MenuItem>
                    </MenuList>
                  </Menu>
                </Td>
              </Tr>
            );
          })}
        </Tbody>
      </Table>
    </TableContainer>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function CutoverStatusPage() {
  const { migrationId } = useParams();
  const toast = useToast();
  const [confirmProps, ask] = useConfirm();

  const [wave, setWave] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const abortRef = useRef(false);

  const fetchWave = useCallback(
    async (showLoader = false) => {
      if (showLoader) setIsLoading(true);
      try {
        const res = await axios.get(
          localRoute(`${constants.CUTOVER_WAVES_ROUTE}/${encodeURIComponent(migrationId)}`),
        );
        if (!abortRef.current) {
          setWave(res.data);
          setError(null);
        }
      } catch (err) {
        const msg = extractError(err);
        if (!abortRef.current) setError(msg);
        if (showLoader) {
          toast({
            title: 'Could not load wave',
            description: msg,
            status: 'error',
            duration: 5000,
            isClosable: true,
            variant: 'outline',
          });
        }
      } finally {
        if (showLoader && !abortRef.current) setIsLoading(false);
      }
    },
    [migrationId, toast],
  );

  useEffect(() => {
    abortRef.current = false;
    fetchWave(true);
    return () => {
      abortRef.current = true;
    };
  }, [fetchWave]);

  const isRunning = wave?.status === 'running';
  useEffect(() => {
    if (!isRunning) return undefined;
    const id = setInterval(() => fetchWave(false), 3000);
    return () => clearInterval(id);
  }, [isRunning, fetchWave]);

  // --- Action dispatch ---
  // Every action posts to an endpoint, toasts on success/error, and refetches
  // the wave so the UI reflects the new state. Destructive actions go through
  // the confirm dialog first.
  const postAction = useCallback(
    async ({ path, body, successTitle }) => {
      try {
        const res = await axios.post(
          localRoute(`${constants.CUTOVER_WAVES_ROUTE}/${encodeURIComponent(migrationId)}${path}`),
          body ?? {},
        );
        toast({
          title: successTitle,
          description: typeof res.data === 'object' ? JSON.stringify(res.data) : String(res.data ?? ''),
          status: 'success',
          duration: 4000,
          isClosable: true,
          variant: 'outline',
        });
        fetchWave(false);
      } catch (err) {
        toast({
          title: 'Action failed',
          description: extractError(err),
          status: 'error',
          duration: 8000,
          isClosable: true,
          variant: 'outline',
        });
        throw err; // Let useConfirm keep the dialog open.
      }
    },
    [migrationId, toast, fetchWave],
  );

  const handleWaveAction = useCallback(
    (action) => {
      switch (action) {
        case 'abort':
          ask({
            title: 'Abort this wave?',
            body: (
              <>
                The wave will stop scheduling new DAGs. Any DAG currently mid-write finishes its current step cleanly.
                Pending DAGs become <Code>aborted</Code>.
              </>
            ),
            confirmLabel: 'Abort wave',
            colorScheme: 'orange',
            onConfirm: () => postAction({ path: '/abort', successTitle: 'Abort requested' }),
          });
          break;
        case 'retry-failed':
          ask({
            title: 'Retry failed DAGs?',
            body: 'Rolls back any partial data for failed DAGs and re-runs them with the wave’s original config.',
            confirmLabel: 'Retry failed',
            colorScheme: 'blue',
            onConfirm: () =>
              postAction({ path: '/retry', body: { selector: 'failed' }, successTitle: 'Retry queued' }),
          });
          break;
        case 'retry-skipped':
          ask({
            title: 'Retry skipped DAGs?',
            body: 'Re-runs DAGs that were deferred past max_retries due to active runs in source.',
            confirmLabel: 'Retry skipped',
            colorScheme: 'blue',
            onConfirm: () =>
              postAction({ path: '/retry', body: { selector: 'skipped' }, successTitle: 'Retry queued' }),
          });
          break;
        case 'rollback-wave':
          ask({
            title: 'Roll back the whole wave?',
            body: (
              <>
                Deletes migrated <Code>dag_runs</Code>, <Code>task_instances</Code>, and TI history for every
                completed/failed DAG in this wave. Reverses the pause/unpause this wave applied. Cannot be undone —
                re-run the wave if you change your mind.
              </>
            ),
            confirmLabel: 'Roll back wave',
            colorScheme: 'red',
            onConfirm: () => postAction({ path: '/rollback', successTitle: 'Wave rolled back' }),
          });
          break;
        case 'purge-wave':
          ask({
            title: 'Purge wave metadata?',
            body: (
              <>
                Force-deletes <strong>ALL</strong> <Code>dag_runs</Code>, <Code>task_instances</Code>, and TI history
                for every DAG in this wave (not just migrated rows). Use this only when rollback isn&apos;t enough —
                e.g. to clear partial data before re-running.
              </>
            ),
            confirmLabel: 'Purge wave',
            colorScheme: 'red',
            onConfirm: () => postAction({ path: '/purge', successTitle: 'Purge complete' }),
          });
          break;
        default:
          break;
      }
    },
    [ask, postAction],
  );

  const handleDagAction = useCallback(
    (action, dagId) => {
      switch (action) {
        case 'retry':
          ask({
            title: `Retry DAG '${dagId}'?`,
            body: 'Rolls back any partial data for this DAG and re-runs it.',
            confirmLabel: 'Retry',
            colorScheme: 'blue',
            onConfirm: () =>
              postAction({ path: '/retry', body: { selector: dagId }, successTitle: 'Retry queued' }),
          });
          break;
        case 'rollback':
          ask({
            title: `Roll back DAG '${dagId}'?`,
            body: (
              <>
                Deletes the migrated <Code>dag_runs</Code>, <Code>task_instances</Code>, and TI history for this DAG.
                Reverses the pause/unpause this wave applied to it.
              </>
            ),
            confirmLabel: 'Roll back DAG',
            colorScheme: 'red',
            onConfirm: () => postAction({ path: '/rollback', body: { dag_id: dagId }, successTitle: 'DAG rolled back' }),
          });
          break;
        case 'purge':
          ask({
            title: `Purge metadata for '${dagId}'?`,
            body: (
              <>
                Force-deletes <strong>ALL</strong> metadata for this DAG on this Airflow, regardless of when it was
                written. Use this only when rollback leaves residue you need to clear.
              </>
            ),
            confirmLabel: 'Purge DAG',
            colorScheme: 'red',
            onConfirm: () => postAction({ path: '/purge', body: { dag_id: dagId }, successTitle: 'DAG purged' }),
          });
          break;
        default:
          break;
      }
    },
    [ask, postAction],
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
          <HStack mb={1}>
            <Button
              as={NavLink}
              to={`/${ROUTES.CUTOVER}`}
              size="xs"
              variant="ghost"
              leftIcon={<ArrowBackIcon />}
            >
              Back to Cutover
            </Button>
          </HStack>
          <Heading size="md">Wave status</Heading>
          <Text fontFamily="mono" fontSize="xs" color="gray.600">
            {migrationId}
          </Text>
        </Box>
        <HStack>
          {isRunning && <Spinner size="sm" color="info.500" />}
          <Tooltip label="Refresh" placement="top" hasArrow>
            <IconButton
              size="sm"
              variant="outline"
              aria-label="Refresh"
              icon={<RepeatIcon />}
              onClick={() => fetchWave(true)}
            />
          </Tooltip>
        </HStack>
      </Stack>

      {isLoading && !wave ? (
        <Box py={10} display="flex" justifyContent="center">
          <Spinner />
        </Box>
      ) : error && !wave ? (
        <Alert status="error" variant="left-accent">
          <AlertIcon />
          <Box>
            <Text fontWeight="semibold" fontSize="sm">
              Could not load this wave.
            </Text>
            <Text fontSize="xs">{error}</Text>
            <Text fontSize="xs" mt={1}>
              Head back to the{' '}
              <Link as={NavLink} to={`/${ROUTES.CUTOVER}`} color="brand.500" fontWeight="semibold">
                Cutover
              </Link>{' '}
              page to pick a different wave.
            </Text>
          </Box>
        </Alert>
      ) : wave ? (
        <VStack align="stretch" spacing={4}>
          <Card>
            <CardBody>
              <Stack
                direction={{ base: 'column', md: 'row' }}
                justify="space-between"
                align={{ base: 'flex-start', md: 'center' }}
                mb={3}
              >
                <HStack spacing={4}>
                  <Badge colorScheme={wave.type === 'bigbang' ? 'amethyst' : 'brand'} fontSize="sm">
                    {wave.type}
                  </Badge>
                  <Badge colorScheme={STATUS_COLORS[wave.status] || 'gray'} fontSize="sm">
                    {wave.status}
                  </Badge>
                  {wave.abort_requested && <Badge colorScheme="warning">abort requested</Badge>}
                </HStack>
                <VStack align="flex-end" spacing={0}>
                  <Text fontSize="xs" color="gray.600">
                    Started {formatTimestamp(wave.started_at)}
                  </Text>
                  {wave.completed_at && (
                    <Text fontSize="xs" color="gray.600">
                      Finished {formatTimestamp(wave.completed_at)}
                    </Text>
                  )}
                </VStack>
              </Stack>
              <SummaryGrid summary={wave.summary} />
              <Box mt={3}>
                <ProgressBar summary={wave.summary} />
              </Box>
              <Box mt={4}>
                <Text fontSize="xs" color="gray.600" mb={2}>
                  Wave actions
                </Text>
                <WaveActions wave={wave} onAction={handleWaveAction} />
              </Box>
            </CardBody>
          </Card>

          <Card>
            <CardBody>
              <HStack justify="space-between" mb={3}>
                <Heading size="sm">Per-DAG progress</Heading>
                <Text fontSize="xs" color="gray.500">
                  {isRunning ? 'Auto-refreshing every 3s while running' : 'Static snapshot'}
                </Text>
              </HStack>
              <DagTable dags={wave.dags} isWaveRunning={isRunning} onDagAction={handleDagAction} />
            </CardBody>
          </Card>

          <Card variant="outline">
            <CardBody>
              <Heading size="xs" mb={2}>
                Wave configuration
              </Heading>
              <Code
                display="block"
                whiteSpace="pre"
                fontSize="xs"
                p={3}
                borderRadius="md"
                bg="gray.50"
                overflowX="auto"
              >
                {JSON.stringify(wave.config, null, 2)}
              </Code>
            </CardBody>
          </Card>
        </VStack>
      ) : null}

      <ConfirmDialog {...confirmProps} />
    </Box>
  );
}
