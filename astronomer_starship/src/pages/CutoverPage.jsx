import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  AlertIcon,
  Badge,
  Box,
  Button,
  Card,
  CardBody,
  Checkbox,
  Code,
  Collapse,
  Divider,
  FormControl,
  FormHelperText,
  FormLabel,
  Heading,
  HStack,
  IconButton,
  Link,
  NumberDecrementStepper,
  NumberIncrementStepper,
  NumberInput,
  NumberInputField,
  NumberInputStepper,
  Radio,
  RadioGroup,
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
  Textarea,
  Th,
  Thead,
  Tooltip,
  Tr,
  useDisclosure,
  useToast,
  VStack,
} from '@chakra-ui/react';
import { ChevronDownIcon, ChevronUpIcon, InfoIcon, RepeatIcon, TimeIcon, WarningTwoIcon } from '@chakra-ui/icons';
import axios from 'axios';
import { NavLink } from 'react-router-dom';
import { useSourceConfig, useSourceSetupComplete } from '../AppContext';
import constants, { ROUTES } from '../constants';
import { localRoute } from '../util';
import ConfirmDialog from '../component/ConfirmDialog';
import useConfirm from '../hooks/useConfirm';

const STRATEGIES = [
  {
    id: 'incremental',
    label: 'Incremental wave',
    hint: 'Migrate a specific batch of DAGs matched by patterns. Recommended for large or phased migrations — run as many waves as you need.',
  },
  {
    id: 'bigbang',
    label: 'Big-bang',
    hint: 'Migrate every DAG present on both source and target in one shot. Best for small environments or a final cutover.',
  },
];

const STATUS_COLORS = {
  running: 'info',
  completed: 'success',
  failed: 'error',
  aborted: 'warning',
  rolled_back: 'warning',
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

function buildSummary(dagDict) {
  const statuses = Object.values(dagDict || {}).map((d) => d.status);
  const count = (s) => statuses.filter((x) => x === s).length;
  return {
    total: statuses.length,
    completed: count('completed'),
    failed: count('failed'),
    running: count('running'),
    pending: count('pending'),
    rolled_back: count('rolled_back'),
  };
}

// ---------------------------------------------------------------------------
// Getting Started panel — first-time-user context, explicit by design.
// ---------------------------------------------------------------------------

function GettingStarted() {
  const disclosure = useDisclosure({ defaultIsOpen: true });
  return (
    <Card variant="outline" borderColor="brand.100" bg="brand.10">
      <CardBody py={3}>
        <HStack
          justify="space-between"
          cursor="pointer"
          onClick={disclosure.onToggle}
          _hover={{ bg: 'brand.25' }}
          borderRadius="md"
          px={2}
          py={1}
          mx={-2}
        >
          <HStack>
            <InfoIcon color="brand.500" />
            <Heading size="sm">Getting Started</Heading>
          </HStack>
          <IconButton
            size="xs"
            variant="ghost"
            aria-label={disclosure.isOpen ? 'Collapse' : 'Expand'}
            icon={disclosure.isOpen ? <ChevronUpIcon /> : <ChevronDownIcon />}
          />
        </HStack>
        <Collapse in={disclosure.isOpen} animateOpacity>
          <VStack align="stretch" spacing={3} mt={3}>
            <Text fontSize="sm">
              The Cutover Tool migrates DAG metadata (DAG runs, task instances, TI history) from your source
              Airflow <strong>into this Airflow</strong>, in waves you can observe, roll back, and retry.
            </Text>

            <Box>
              <Text fontSize="sm" fontWeight="semibold" mb={1}>
                What is a wave?
              </Text>
              <Text fontSize="xs" color="gray.700">
                A wave is a batch of DAGs migrated together. You can run as many waves as you need — each one is an
                independently observable, reversible unit of work. Waves are a first-class object: you can watch
                progress per-DAG, roll back a bad wave, retry the DAGs that failed, or abort mid-flight.
              </Text>
            </Box>

            <Box>
              <Text fontSize="sm" fontWeight="semibold" mb={1}>
                When to pick Incremental vs Big-bang
              </Text>
              <VStack align="stretch" spacing={1} fontSize="xs" color="gray.700">
                <Text>
                  <Badge colorScheme="brand">Incremental</Badge> — a targeted batch selected by fnmatch patterns
                  (e.g. <Code fontSize="2xs">etl_*</Code>). Almost always the right choice for a real migration.
                </Text>
                <Text>
                  <Badge colorScheme="amethyst">Big-bang</Badge> — everything at once. Use for small environments
                  or the final wave of a phased migration.
                </Text>
              </VStack>
            </Box>

            <Box>
              <Text fontSize="sm" fontWeight="semibold" mb={1}>
                Safety
              </Text>
              <VStack align="stretch" spacing={0.5} fontSize="xs" color="gray.700">
                <Text>• Pre-checks ensure each DAG is paused and has no runs on this Airflow before writing.</Text>
                <Text>• Pause on source + unpause on target can be coordinated atomically per DAG.</Text>
                <Text>• Rollback removes migrated rows and reverses pause/unpause — per DAG or per wave.</Text>
                <Text>• Abort stops a running wave; in-flight writes finish cleanly.</Text>
              </VStack>
            </Box>
          </VStack>
        </Collapse>
      </CardBody>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Launch form — strategy / patterns / advanced config.
// ---------------------------------------------------------------------------

const DEFAULT_CONFIG = {
  dag_run_limit: 500,
  parallel_workers: 4,
  pause_in_source: true,
  unpause_in_target: false,
  wait_for_scheduler: false,
  wait_for_running: false,
  retry_interval: 120,
  max_retries: 3,
};

function LaunchForm({ onLaunched }) {
  const toast = useToast();
  const advanced = useDisclosure({ defaultIsOpen: false });
  const [strategy, setStrategy] = useState('incremental');
  const [patternsRaw, setPatternsRaw] = useState('');
  const [config, setConfig] = useState(DEFAULT_CONFIG);
  const [submitting, setSubmitting] = useState(false);

  const patterns = useMemo(
    () =>
      patternsRaw
        .split('\n')
        .map((p) => p.trim())
        .filter(Boolean),
    [patternsRaw],
  );

  const patternsLabel = strategy === 'incremental' ? 'Include patterns' : 'Exclude patterns (optional)';
  const patternsHelper =
    strategy === 'incremental'
      ? 'One fnmatch pattern per line. Only DAGs matching any of these patterns will be migrated. Required for incremental waves.'
      : 'One fnmatch pattern per line. DAGs matching any of these will be SKIPPED from the big-bang wave. Leave empty to migrate everything.';
  const canSubmit = strategy === 'incremental' ? patterns.length > 0 : true;

  const setConfigField = (key, value) => setConfig((c) => ({ ...c, [key]: value }));

  const handleSubmit = async () => {
    setSubmitting(true);
    try {
      const res = await axios.post(localRoute(constants.CUTOVER_WAVES_ROUTE), {
        strategy,
        patterns,
        config,
      });
      toast({
        title: 'Wave launched',
        description: `${res.data?.dags ? Object.keys(res.data.dags).length : '—'} DAGs queued. Wave id: ${
          res.data?.id || 'n/a'
        }`,
        status: 'success',
        duration: 5000,
        isClosable: true,
        variant: 'outline',
      });
      onLaunched?.(res.data);
      // Reset patterns so the user can't accidentally re-run the same wave.
      setPatternsRaw('');
    } catch (err) {
      const raw = err.response?.data?.error || err.message || 'Unknown error';
      toast({
        title: 'Failed to launch wave',
        description: typeof raw === 'string' ? raw : JSON.stringify(raw),
        status: 'error',
        duration: 8000,
        isClosable: true,
        variant: 'outline',
      });
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Card>
      <CardBody>
        <VStack align="stretch" spacing={4}>
          <Box>
            <Heading size="sm" mb={1}>
              Launch a wave
            </Heading>
            <Text fontSize="xs" color="gray.600">
              Pick a strategy, list the DAGs, then hit <strong>Launch wave</strong>. You&apos;ll be able to watch
              progress and roll back from the status view.
            </Text>
          </Box>

          <FormControl>
            <FormLabel fontSize="sm" mb={2}>
              Strategy
            </FormLabel>
            <RadioGroup value={strategy} onChange={setStrategy}>
              <VStack align="stretch" spacing={2}>
                {STRATEGIES.map((s) => {
                  const isSelected = strategy === s.id;
                  return (
                    <Box
                      key={s.id}
                      as="label"
                      htmlFor={`cutover-strategy-${s.id}`}
                      borderWidth="1px"
                      borderColor={isSelected ? 'brand.500' : 'gray.200'}
                      bg={isSelected ? 'brand.50' : 'white'}
                      borderRadius="md"
                      px={3}
                      py={2}
                      cursor="pointer"
                      transition="all 0.15s"
                      _hover={{ borderColor: 'brand.400' }}
                    >
                      <HStack align="flex-start" spacing={3}>
                        <Radio id={`cutover-strategy-${s.id}`} value={s.id} mt={1} />
                        <Box>
                          <Text fontWeight="semibold" fontSize="sm">
                            {s.label}
                          </Text>
                          <Text fontSize="xs" color="gray.600">
                            {s.hint}
                          </Text>
                        </Box>
                      </HStack>
                    </Box>
                  );
                })}
              </VStack>
            </RadioGroup>
          </FormControl>

          <FormControl>
            <FormLabel fontSize="sm" mb={1}>
              {patternsLabel}
            </FormLabel>
            <Textarea
              size="sm"
              rows={4}
              placeholder={'etl_*\nreporting_daily\nops_hourly_*'}
              value={patternsRaw}
              onChange={(e) => setPatternsRaw(e.target.value)}
              fontFamily="mono"
              fontSize="xs"
            />
            <FormHelperText fontSize="xs">{patternsHelper}</FormHelperText>
          </FormControl>

          <Box>
            <HStack
              justify="space-between"
              cursor="pointer"
              onClick={advanced.onToggle}
              _hover={{ bg: 'gray.50' }}
              borderRadius="md"
              px={2}
              py={1}
              mx={-2}
            >
              <HStack>
                <Text fontSize="sm" fontWeight="semibold">
                  Advanced options
                </Text>
                <Tooltip
                  label="Defaults work for most migrations. Open this only if you know what you're changing."
                  placement="top"
                  hasArrow
                >
                  <InfoIcon color="gray.400" boxSize={3} cursor="help" />
                </Tooltip>
              </HStack>
              <IconButton
                size="xs"
                variant="ghost"
                aria-label={advanced.isOpen ? 'Collapse' : 'Expand'}
                icon={advanced.isOpen ? <ChevronUpIcon /> : <ChevronDownIcon />}
              />
            </HStack>
            <Collapse in={advanced.isOpen} animateOpacity>
              <VStack align="stretch" spacing={3} mt={3}>
                <Stack direction={{ base: 'column', md: 'row' }} spacing={4}>
                  <NumberField
                    label="DAG run limit"
                    helper="Max DAG runs to fetch per DAG."
                    value={config.dag_run_limit}
                    onChange={(v) => setConfigField('dag_run_limit', v)}
                    min={1}
                    max={100000}
                  />
                  <NumberField
                    label="Parallel workers"
                    helper="Number of DAGs migrated concurrently. Capped at 16."
                    value={config.parallel_workers}
                    onChange={(v) => setConfigField('parallel_workers', v)}
                    min={1}
                    max={16}
                  />
                </Stack>

                <Divider />

                <VStack align="stretch" spacing={2}>
                  <Checkbox
                    size="sm"
                    isChecked={config.pause_in_source}
                    onChange={(e) => setConfigField('pause_in_source', e.target.checked)}
                  >
                    <Text fontSize="sm">Pause each DAG on source after migration</Text>
                    <Text fontSize="xs" color="gray.600">
                      Prevents double-scheduling during the cutover. Reversed automatically on rollback.
                    </Text>
                  </Checkbox>
                  <Checkbox
                    size="sm"
                    isChecked={config.unpause_in_target}
                    onChange={(e) => setConfigField('unpause_in_target', e.target.checked)}
                  >
                    <Text fontSize="sm">Unpause each DAG on this Airflow after migration</Text>
                    <Text fontSize="xs" color="gray.600">
                      Activates DAGs as soon as their metadata lands. Leave unchecked to verify manually first.
                    </Text>
                  </Checkbox>
                  <Checkbox
                    size="sm"
                    isChecked={config.wait_for_scheduler}
                    onChange={(e) => setConfigField('wait_for_scheduler', e.target.checked)}
                  >
                    <Text fontSize="sm">Wait for this Airflow&apos;s scheduler to sync next_dagrun</Text>
                    <Text fontSize="xs" color="gray.600">
                      Helps avoid the scheduler backfilling missed runs right after migration.
                    </Text>
                  </Checkbox>
                  <Checkbox
                    size="sm"
                    isChecked={config.wait_for_running}
                    onChange={(e) => setConfigField('wait_for_running', e.target.checked)}
                  >
                    <Text fontSize="sm">Defer DAGs that are actively running in source, retry later</Text>
                    <Text fontSize="xs" color="gray.600">
                      Uses <Code fontSize="2xs">retry_interval</Code> and <Code fontSize="2xs">max_retries</Code> below.
                    </Text>
                  </Checkbox>
                </VStack>

                {config.wait_for_running && (
                  <Stack direction={{ base: 'column', md: 'row' }} spacing={4}>
                    <NumberField
                      label="Retry interval (seconds)"
                      helper="How long to wait between retry rounds."
                      value={config.retry_interval}
                      onChange={(v) => setConfigField('retry_interval', v)}
                      min={10}
                      max={3600}
                    />
                    <NumberField
                      label="Max retries"
                      helper="Give up after this many retry rounds."
                      value={config.max_retries}
                      onChange={(v) => setConfigField('max_retries', v)}
                      min={0}
                      max={20}
                    />
                  </Stack>
                )}
              </VStack>
            </Collapse>
          </Box>

          <HStack justify="flex-end" pt={2}>
            <Button
              colorScheme="brand"
              onClick={handleSubmit}
              isLoading={submitting}
              isDisabled={!canSubmit}
              size="sm"
            >
              Launch wave
            </Button>
          </HStack>
        </VStack>
      </CardBody>
    </Card>
  );
}

function NumberField({ label, helper, value, onChange, min, max }) {
  return (
    <FormControl>
      <FormLabel fontSize="sm" mb={1}>
        {label}
      </FormLabel>
      <NumberInput
        size="sm"
        value={value}
        onChange={(_, asNumber) => onChange(Number.isNaN(asNumber) ? min : asNumber)}
        min={min}
        max={max}
      >
        <NumberInputField />
        <NumberInputStepper>
          <NumberIncrementStepper />
          <NumberDecrementStepper />
        </NumberInputStepper>
      </NumberInput>
      {helper && <FormHelperText fontSize="xs">{helper}</FormHelperText>}
    </FormControl>
  );
}

// ---------------------------------------------------------------------------
// Recent waves list — polls while any wave is running.
// ---------------------------------------------------------------------------

function RecentWaves({ waves, isLoading, onRefresh }) {
  return (
    <Card>
      <CardBody>
        <HStack justify="space-between" mb={3}>
          <Box>
            <Heading size="sm">Recent waves</Heading>
            <Text fontSize="xs" color="gray.600">
              The 25 most recent waves on this Airflow. Click a wave to open its status view.
            </Text>
          </Box>
          <HStack>
            {isLoading && <Spinner size="xs" color="brand.400" />}
            <Tooltip label="Refresh" placement="top" hasArrow>
              <IconButton
                size="xs"
                variant="ghost"
                aria-label="Refresh"
                icon={<RepeatIcon />}
                onClick={onRefresh}
              />
            </Tooltip>
          </HStack>
        </HStack>
        {waves.length === 0 ? (
          <Text fontSize="sm" color="gray.500" fontStyle="italic">
            No waves yet — launch one above to see it here.
          </Text>
        ) : (
          <TableContainer>
            <Table size="sm" variant="striped">
              <Thead>
                <Tr>
                  <Th>Wave id</Th>
                  <Th>Strategy</Th>
                  <Th>Status</Th>
                  <Th>Progress</Th>
                  <Th>Started</Th>
                </Tr>
              </Thead>
              <Tbody>
                {waves.map((w) => {
                  const summary = buildSummary(w.dags);
                  return (
                    <Tr key={w.id}>
                      <Td>
                        <Link
                          as={NavLink}
                          to={`/${ROUTES.CUTOVER}/${encodeURIComponent(w.id)}`}
                          fontFamily="mono"
                          fontSize="xs"
                          color="brand.500"
                        >
                          {w.id}
                        </Link>
                      </Td>
                      <Td>
                        <Badge colorScheme={w.type === 'bigbang' ? 'amethyst' : 'brand'}>{w.type}</Badge>
                      </Td>
                      <Td>
                        <Badge colorScheme={STATUS_COLORS[w.status] || 'gray'}>{w.status}</Badge>
                      </Td>
                      <Td>
                        <Text fontSize="xs">
                          {summary.completed}/{summary.total}{' '}
                          {summary.failed > 0 && (
                            <Text as="span" color="error.500">
                              ({summary.failed} failed)
                            </Text>
                          )}
                          {summary.running > 0 && (
                            <Text as="span" color="info.500">
                              ({summary.running} running)
                            </Text>
                          )}
                        </Text>
                      </Td>
                      <Td>
                        <HStack fontSize="xs" color="gray.600">
                          <TimeIcon boxSize={3} />
                          <Text>{formatTimestamp(w.started_at)}</Text>
                        </HStack>
                      </Td>
                    </Tr>
                  );
                })}
              </Tbody>
            </Table>
          </TableContainer>
        )}
      </CardBody>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Danger zone — instance-wide escape hatches. Intentionally ugly so it feels
// consequential. Collapsed by default.
// ---------------------------------------------------------------------------

function DangerZone({ onPurgeAll }) {
  const disclosure = useDisclosure({ defaultIsOpen: false });
  return (
    <Card variant="outline" borderColor="error.200" bg="error.50">
      <CardBody py={3}>
        <HStack
          justify="space-between"
          cursor="pointer"
          onClick={disclosure.onToggle}
          _hover={{ bg: 'error.100' }}
          borderRadius="md"
          px={2}
          py={1}
          mx={-2}
        >
          <HStack>
            <WarningTwoIcon color="error.500" />
            <Heading size="sm" color="error.700">
              Danger zone
            </Heading>
          </HStack>
          <IconButton
            size="xs"
            variant="ghost"
            aria-label={disclosure.isOpen ? 'Collapse' : 'Expand'}
            icon={disclosure.isOpen ? <ChevronUpIcon /> : <ChevronDownIcon />}
          />
        </HStack>
        <Collapse in={disclosure.isOpen} animateOpacity>
          <VStack align="stretch" spacing={3} mt={3}>
            <Text fontSize="xs" color="error.700">
              Instance-wide escape hatches. These actions are not scoped to a single wave and cannot be undone. Use
              them only when rollback or per-wave purge aren&apos;t enough.
            </Text>
            <HStack justify="space-between" borderWidth="1px" borderColor="error.200" bg="white" p={3} borderRadius="md">
              <Box>
                <Text fontSize="sm" fontWeight="semibold">
                  Purge ALL destination DAG metadata
                </Text>
                <Text fontSize="xs" color="gray.700">
                  Deletes every <Code fontSize="2xs">dag_run</Code>, <Code fontSize="2xs">task_instance</Code>, and TI
                  history row on this Airflow — for every DAG, regardless of which wave (if any) migrated them. Use
                  this to start from a blank slate.
                </Text>
              </Box>
              <Button size="sm" colorScheme="red" onClick={onPurgeAll}>
                Purge all
              </Button>
            </HStack>
          </VStack>
        </Collapse>
      </CardBody>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Page shell
// ---------------------------------------------------------------------------

function SummaryStrip({ waves }) {
  const agg = useMemo(() => {
    const out = { total: 0, running: 0, completed: 0, failed: 0 };
    waves.forEach((w) => {
      out.total += 1;
      if (w.status === 'running') out.running += 1;
      else if (w.status === 'completed') out.completed += 1;
      else if (w.status === 'failed' || w.status === 'aborted') out.failed += 1;
    });
    return out;
  }, [waves]);

  return (
    <HStack spacing={6} wrap="wrap">
      <Stat size="sm">
        <StatLabel fontSize="xs">Waves on record</StatLabel>
        <StatNumber fontSize="lg">{agg.total}</StatNumber>
      </Stat>
      <Stat size="sm">
        <StatLabel fontSize="xs" color="info.500">
          In flight
        </StatLabel>
        <StatNumber fontSize="lg" color="info.500">
          {agg.running}
        </StatNumber>
      </Stat>
      <Stat size="sm">
        <StatLabel fontSize="xs" color="success.500">
          Completed
        </StatLabel>
        <StatNumber fontSize="lg" color="success.500">
          {agg.completed}
        </StatNumber>
      </Stat>
      <Stat size="sm">
        <StatLabel fontSize="xs" color="error.500">
          Failed / aborted
        </StatLabel>
        <StatNumber fontSize="lg" color="error.500">
          {agg.failed}
        </StatNumber>
      </Stat>
    </HStack>
  );
}

export default function CutoverPage() {
  const source = useSourceConfig();
  const isSourceReady = useSourceSetupComplete();
  const toast = useToast();
  const [confirmProps, ask] = useConfirm();

  const [waves, setWaves] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  // Track the latest successful fetch so a failing poll doesn't flash "no waves".
  const lastGoodRef = useRef([]);

  const fetchWaves = useCallback(
    async (showLoader = false) => {
      if (showLoader) setIsLoading(true);
      try {
        const res = await axios.get(localRoute(constants.CUTOVER_WAVES_ROUTE), { params: { limit: 25 } });
        const next = res.data?.migrations || [];
        setWaves(next);
        lastGoodRef.current = next;
      } catch (err) {
        if (showLoader) {
          const raw = err.response?.data?.error || err.message || 'Unknown error';
          toast({
            title: 'Could not load waves',
            description: typeof raw === 'string' ? raw : JSON.stringify(raw),
            status: 'error',
            duration: 5000,
            isClosable: true,
            variant: 'outline',
          });
        }
      } finally {
        if (showLoader) setIsLoading(false);
      }
    },
    [toast],
  );

  useEffect(() => {
    fetchWaves(true);
  }, [fetchWaves]);

  // Poll while any wave is in flight — cheaper than a constant interval.
  const hasInFlight = waves.some((w) => w.status === 'running');
  useEffect(() => {
    if (!hasInFlight) return undefined;
    const id = setInterval(() => fetchWaves(false), 5000);
    return () => clearInterval(id);
  }, [hasInFlight, fetchWaves]);

  const handleLaunched = useCallback(
    (newWave) => {
      // Optimistically prepend so the user sees the launch immediately even
      // if the next poll is a few seconds out.
      if (newWave?.id) {
        setWaves((prev) => [newWave, ...prev.filter((w) => w.id !== newWave.id)]);
      }
      fetchWaves(false);
    },
    [fetchWaves],
  );

  const handlePurgeAll = useCallback(() => {
    ask({
      title: 'Purge ALL destination DAG metadata?',
      body: (
        <>
          This deletes every <Code>dag_run</Code>, <Code>task_instance</Code>, and TI history row on this Airflow —
          for every DAG, including ones that were never part of a cutover wave. <strong>It cannot be undone.</strong>{' '}
          Make sure you have a fresh database backup.
        </>
      ),
      confirmLabel: 'Purge everything',
      colorScheme: 'red',
      onConfirm: async () => {
        try {
          const res = await axios.post(localRoute(constants.CUTOVER_PURGE_ALL_ROUTE));
          toast({
            title: 'Purge complete',
            description: `Purged ${res.data?.purged ?? 0} DAGs${res.data?.errors ? `, ${res.data.errors} errors` : ''}.`,
            status: 'success',
            duration: 6000,
            isClosable: true,
            variant: 'outline',
          });
          fetchWaves(false);
        } catch (err) {
          const raw = err.response?.data?.error || err.message || 'Unknown error';
          toast({
            title: 'Purge failed',
            description: typeof raw === 'string' ? raw : JSON.stringify(raw),
            status: 'error',
            duration: 8000,
            isClosable: true,
            variant: 'outline',
          });
          throw err;
        }
      },
    });
  }, [ask, toast, fetchWaves]);

  if (!isSourceReady) {
    return (
      <Box>
        <Alert status="warning" variant="left-accent">
          <AlertIcon />
          <Box>
            <Text fontWeight="semibold" fontSize="sm">
              Source Setup is not complete.
            </Text>
            <Text fontSize="xs">
              The Cutover Tool needs a source Airflow connection. Head over to{' '}
              <Link as={NavLink} to={`/${ROUTES.SOURCE_SETUP}`} color="brand.500" fontWeight="semibold">
                Source Setup
              </Link>{' '}
              to configure it.
            </Text>
          </Box>
        </Alert>
      </Box>
    );
  }

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
            Cutover
          </Heading>
          <Text fontSize="xs" color="gray.600">
            Migrating from <strong>{source.url}</strong> ({source.platform}) into this Airflow.
          </Text>
        </Box>
        <SummaryStrip waves={waves} />
      </Stack>

      <VStack align="stretch" spacing={4}>
        <GettingStarted />
        <LaunchForm onLaunched={handleLaunched} />
        <RecentWaves waves={waves} isLoading={isLoading} onRefresh={() => fetchWaves(true)} />
        <DangerZone onPurgeAll={handlePurgeAll} />
      </VStack>
      <ConfirmDialog {...confirmProps} />
    </Box>
  );
}
