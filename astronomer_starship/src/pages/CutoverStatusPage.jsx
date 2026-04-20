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
import { ArrowBackIcon, RepeatIcon } from '@chakra-ui/icons';
import axios from 'axios';
import { NavLink, useParams } from 'react-router-dom';
import constants, { ROUTES } from '../constants';
import { localRoute } from '../util';

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

function formatTimestamp(iso) {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
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

function DagTable({ dags }) {
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
          </Tr>
        </Thead>
        <Tbody>
          {rows.map((r) => (
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
            </Tr>
          ))}
        </Tbody>
      </Table>
    </TableContainer>
  );
}

export default function CutoverStatusPage() {
  const { migrationId } = useParams();
  const toast = useToast();
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
        const raw = err.response?.data?.error || err.message || 'Unknown error';
        if (!abortRef.current) setError(typeof raw === 'string' ? raw : JSON.stringify(raw));
        if (showLoader) {
          toast({
            title: 'Could not load wave',
            description: typeof raw === 'string' ? raw : JSON.stringify(raw),
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
            <IconButton size="sm" variant="outline" aria-label="Refresh" icon={<RepeatIcon />} onClick={() => fetchWave(true)} />
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
              <DagTable dags={wave.dags} />
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

          <Alert status="info" variant="subtle" fontSize="xs" borderRadius="md">
            <AlertIcon />
            <Text>
              Action buttons (abort, rollback, retry, purge) will be added here in the next iteration. For now,
              operators can call the REST API directly.
            </Text>
          </Alert>
        </VStack>
      ) : null}
    </Box>
  );
}
