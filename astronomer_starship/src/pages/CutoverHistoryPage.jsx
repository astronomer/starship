import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  AlertIcon,
  Badge,
  Box,
  Button,
  Card,
  CardBody,
  Code,
  Collapse,
  Heading,
  HStack,
  IconButton,
  Link,
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
  useDisclosure,
  useToast,
  VStack,
} from '@chakra-ui/react';
import { ChevronDownIcon, ChevronUpIcon, RepeatIcon, TimeIcon, WarningTwoIcon } from '@chakra-ui/icons';
import axios from 'axios';
import PropTypes from 'prop-types';
import { NavLink } from 'react-router-dom';
import constants, { CUTOVER_STATUS_COLORS, ROUTES } from '../constants';
import { extractAxiosError, localRoute } from '../util';
import { useSourceSetupComplete } from '../AppContext';
import ConfirmDialog from '../component/ConfirmDialog';
import useConfirm from '../hooks/useConfirm';

function formatTimestamp(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
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

SummaryStrip.propTypes = { waves: PropTypes.arrayOf(PropTypes.object).isRequired };

function WavesTable({ waves, isLoading, onRefresh }) {
  return (
    <Card>
      <CardBody>
        <HStack justify="space-between" mb={3}>
          <Box>
            <Heading size="sm">All waves</Heading>
            <Text fontSize="xs" color="gray.600">
              The 25 most recent waves on this Airflow. Click a wave to open its status view.
            </Text>
          </Box>
          <HStack>
            {isLoading && <Spinner size="xs" color="brand.400" />}
            <Tooltip label="Refresh" placement="top" hasArrow>
              <IconButton size="xs" variant="ghost" aria-label="Refresh" icon={<RepeatIcon />} onClick={onRefresh} />
            </Tooltip>
          </HStack>
        </HStack>
        {waves.length === 0 ? (
          <Text fontSize="sm" color="gray.500" fontStyle="italic">
            No waves yet. Head over to{' '}
            <Link as={NavLink} to={`/${ROUTES.CUTOVER}`} color="brand.500" fontWeight="semibold">
              Cutover
            </Link>{' '}
            to launch one.
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
                        <Badge colorScheme={CUTOVER_STATUS_COLORS[w.status] || 'gray'}>{w.status}</Badge>
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

WavesTable.propTypes = {
  waves: PropTypes.arrayOf(PropTypes.object).isRequired,
  isLoading: PropTypes.bool.isRequired,
  onRefresh: PropTypes.func.isRequired,
};

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
              Instance-wide escape hatches. These actions are not scoped to a single wave and cannot be undone. Use them
              only when rollback or per-wave purge aren&apos;t enough.
            </Text>
            <HStack
              justify="space-between"
              borderWidth="1px"
              borderColor="error.200"
              bg="white"
              p={3}
              borderRadius="md"
            >
              <Box>
                <Text fontSize="sm" fontWeight="semibold">
                  Purge ALL destination DAG metadata
                </Text>
                <Text fontSize="xs" color="gray.700">
                  Deletes every <Code fontSize="2xs">dag_run</Code>, <Code fontSize="2xs">task_instance</Code>, and TI
                  history row on this Airflow — for every DAG, regardless of which wave (if any) migrated them. Use this
                  to start from a blank slate.
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

DangerZone.propTypes = { onPurgeAll: PropTypes.func.isRequired };

export default function CutoverHistoryPage() {
  const isSourceReady = useSourceSetupComplete();
  const toast = useToast();
  const [confirmProps, ask] = useConfirm();

  const [waves, setWaves] = useState([]);
  const [isLoading, setIsLoading] = useState(false);

  const fetchWaves = useCallback(
    async (showLoader = false) => {
      if (showLoader) setIsLoading(true);
      try {
        const res = await axios.get(localRoute(constants.CUTOVER_WAVES_ROUTE), { params: { limit: 25 } });
        setWaves(res.data?.migrations || []);
      } catch (err) {
        if (showLoader) {
          toast({
            title: 'Could not load waves',
            description: extractAxiosError(err),
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

  const hasInFlight = waves.some((w) => w.status === 'running');
  useEffect(() => {
    if (!hasInFlight) return undefined;
    const id = setInterval(() => fetchWaves(false), 5000);
    return () => clearInterval(id);
  }, [hasInFlight, fetchWaves]);

  const handlePurgeAll = useCallback(() => {
    ask({
      title: 'Purge ALL destination DAG metadata?',
      body: (
        <>
          This deletes every <Code>dag_run</Code>, <Code>task_instance</Code>, and TI history row on this Airflow — for
          every DAG, including ones that were never part of a cutover wave. <strong>It cannot be undone.</strong> Make
          sure you have a fresh database backup.
        </>
      ),
      confirmLabel: 'Purge everything',
      colorScheme: 'red',
      onConfirm: async () => {
        try {
          const res = await axios.post(localRoute(constants.CUTOVER_PURGE_ALL_ROUTE));
          const purged = res.data?.purged ?? 0;
          const errors = res.data?.errors ?? 0;
          toast({
            title: 'Purge complete',
            description: errors ? `Purged ${purged} DAGs, ${errors} errors.` : `Purged ${purged} DAGs.`,
            status: errors ? 'warning' : 'success',
            duration: 6000,
            isClosable: true,
            variant: 'outline',
          });
          fetchWaves(false);
        } catch (err) {
          toast({
            title: 'Purge failed',
            description: extractAxiosError(err),
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
              Finish{' '}
              <Link as={NavLink} to={`/${ROUTES.SOURCE_SETUP}`} color="brand.500" fontWeight="semibold">
                Source Setup
              </Link>{' '}
              to use Cutover.
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
            Cutover History
          </Heading>
          <Text fontSize="xs" color="gray.600">
            Past and in-flight waves on this Airflow. Polls every 5 seconds while any wave is running.
          </Text>
        </Box>
        <SummaryStrip waves={waves} />
      </Stack>

      <VStack align="stretch" spacing={4}>
        <WavesTable waves={waves} isLoading={isLoading} onRefresh={() => fetchWaves(true)} />
        <DangerZone onPurgeAll={handlePurgeAll} />
      </VStack>

      <ConfirmDialog {...confirmProps} />
    </Box>
  );
}
