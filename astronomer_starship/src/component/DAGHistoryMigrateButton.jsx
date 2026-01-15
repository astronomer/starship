 import { useState, useEffect } from 'react';
import {
  Button,
  CircularProgress,
  Flex,
  Icon,
  Text,
  useToast,
} from '@chakra-ui/react';
import PropTypes from 'prop-types';
import axios from 'axios';
import { MdErrorOutline, MdDeleteForever, MdWarning } from 'react-icons/md';
import { GoUpload } from 'react-icons/go';
import { localRoute, proxyHeaders, proxyUrl } from '../util';
import constants from '../constants';
import WithTooltip from './WithTooltip';

/**
 * Centralized button state configuration.
 */
export const BUTTON_STATES = Object.freeze({
  MIGRATE: {
    key: 'MIGRATE',
    buttonText: 'Migrate',
    icon: GoUpload,
    color: 'success.600',
    borderColor: 'success.500',
    hoverBg: 'success.50',
  },
  DELETE: {
    key: 'DELETE',
    buttonText: 'Delete',
    icon: MdDeleteForever,
    color: 'error.600',
    borderColor: 'error.500',
    hoverBg: 'error.50',
  },
  ERROR: {
    key: 'ERROR',
    buttonText: 'Error!',
    icon: MdErrorOutline,
    color: 'error.600',
    borderColor: 'error.500',
    hoverBg: 'error.50',
  },
  MIGRATING: {
    key: 'MIGRATING',
    buttonText: 'Migrating',
    icon: null,
    color: 'success.600',
    borderColor: 'success.500',
    hoverBg: 'success.50',
    progressColor: 'green.400',
    minWidth: '140px',
  },
  DELETING: {
    key: 'DELETING',
    buttonText: 'Deleting',
    icon: null,
    color: 'error.600',
    borderColor: 'error.500',
    hoverBg: 'error.50',
    progressColor: 'red.400',
    minWidth: '140px',
  },
  NOT_IN_REMOTE: {
    key: 'NOT_IN_REMOTE',
    tooltip: 'Deploy DAG to remote before migrating',
    buttonText: 'Not on Remote',
    icon: MdWarning,
    color: 'gray.600',
    borderColor: 'gray.500',
    hoverBg: 'gray.50',
    minWidth: '150px',
  },
  NO_DAG_RUNS: {
    key: 'NO_DAG_RUNS',
    tooltip: 'No DAG Runs to migrate',
    buttonText: 'Nothing to Migrate',
    icon: MdWarning,
    color: 'gray.600',
    borderColor: 'gray.500',
    hoverBg: 'gray.50',
    minWidth: '150px',
  },
});

/**
 * Button for migrating DAG history (runs, task instances, task instance history).
 */
function DAGHistoryMigrateButton({
  url,
  token,
  dagId,
  limit = 1000,
  batchSize = 100,
  existsInRemote = false,
  disabledReason = null,
  onMigrate = null,
  onDelete = null,
}) {
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState(null);
  const [exists, setExists] = useState(existsInRemote);
  const [isDeleting, setIsDeleting] = useState(false);
  const toast = useToast();

  const isLoading = progress > 0 && progress < 100;
  const isDisabled = Boolean(disabledReason);

  // Keep local "exists" state in sync with upstream data when we're not actively mutating it.
  useEffect(() => {
    if (!isLoading) {
      setExists(existsInRemote);
    }
  }, [existsInRemote, isLoading]);

  // Determine current state
  const currentState = (() => {
    if (isLoading) return isDeleting ? BUTTON_STATES.DELETING : BUTTON_STATES.MIGRATING;
    if (disabledReason) return disabledReason;
    if (error) return BUTTON_STATES.ERROR;
    if (exists) return BUTTON_STATES.DELETE;
    return BUTTON_STATES.MIGRATE;
  })();

  const resetState = () => {
    setProgress(0);
    setIsDeleting(false);
  };

  const showError = (err, action = 'migrate') => {
    setExists(false);
    resetState();
    setError(err);
    toast({
      title: `Failed to ${action} DAG history for "${dagId}"`,
      description: err.response?.data?.error || err.response?.data || err.message,
      status: 'error',
      isClosable: true,
      variant: 'outline',
      duration: 6000,
    });
  };

  const completeAction = (newExists) => {
    setProgress(100);
    setExists(newExists);
    setTimeout(resetState, 500);
  };

  const handleDelete = async () => {
    setIsDeleting(true);
    setProgress(50);
    try {
      await axios.delete(proxyUrl(url + constants.DAG_RUNS_ROUTE), {
        headers: proxyHeaders(token),
        params: { dag_id: dagId },
      });
      completeAction(false);
      onDelete?.(dagId);
    } catch (err) {
      showError(err, 'delete');
    }
  };

  const handleMigrate = async () => {
    setProgress(1);

    const migrateBatch = async (offset = 0, totalCount = null, latestRemoteCount = null) => {
      const appliedBatchSize = Math.min(limit - offset, batchSize);
      const params = { dag_id: dagId, limit: appliedBatchSize, offset };

      try {
        const [dagRunsRes, taskInstanceRes, taskInstanceHistoryRes] = await Promise.all([
          axios.get(localRoute(constants.DAG_RUNS_ROUTE), { params }),
          axios.get(localRoute(constants.TASK_INSTANCE_ROUTE), { params }),
          axios.get(localRoute(constants.TASK_INSTANCE_HISTORY_ROUTE), { params }),
        ]);

        const dagRunsToMigrateCount = totalCount || Math.min(dagRunsRes.data.dag_run_count, limit);

        if (dagRunsRes.data.dag_runs.length === 0) {
          const remoteCount = latestRemoteCount ?? 0;
          completeAction(remoteCount > 0);
          onMigrate?.(dagId, remoteCount);
          return;
        }

        const remoteHeaders = { headers: proxyHeaders(token) };
        const remoteParams = { params: { dag_id: dagId } };

        const dagRunCreateRes = await axios.post(
          proxyUrl(url + constants.DAG_RUNS_ROUTE),
          { dag_runs: dagRunsRes.data.dag_runs },
          { ...remoteParams, ...remoteHeaders },
        );

        await axios.post(
          proxyUrl(url + constants.TASK_INSTANCE_ROUTE),
          { task_instances: taskInstanceRes.data.task_instances },
          { ...remoteParams, ...remoteHeaders },
        );

        await axios.post(
          proxyUrl(url + constants.TASK_INSTANCE_HISTORY_ROUTE),
          { task_instances: taskInstanceHistoryRes.data.task_instances },
          { ...remoteParams, ...remoteHeaders },
        );

        const migratedCount = offset + dagRunsRes.data.dag_runs.length;
        const remoteCount = dagRunCreateRes?.data?.dag_run_count ?? latestRemoteCount;

        if (migratedCount < dagRunsToMigrateCount) {
          const newProgress = Math.min(
            99,
            Math.round((migratedCount / dagRunsToMigrateCount) * 100),
          );
          setProgress(newProgress);
          await migrateBatch(offset + appliedBatchSize, dagRunsToMigrateCount, remoteCount);
        } else {
          completeAction(true);
          onMigrate?.(dagId, remoteCount ?? dagRunsToMigrateCount);
        }
      } catch (err) {
        showError(err);
      }
    };

    await migrateBatch(0);
  };

  const handleClick = () => (exists ? handleDelete() : handleMigrate());

  // Render button content
  const renderContent = () => {
    if (isLoading) {
      return (
        <Flex alignItems="center" gap={2}>
          <CircularProgress
            value={progress}
            size="20px"
            thickness="10px"
            color={currentState.progressColor}
            trackColor="gray.200"
            isIndeterminate={progress < 2}
          />
          <Text fontSize="xs">
            {isDeleting ? 'Deleting' : 'Migrating'}
            {' '}
            {Math.round(progress)}
            %
          </Text>
        </Flex>
      );
    }
    return (
      <Flex alignItems="center" gap={2}>
        {currentState.icon && <Icon as={currentState.icon} boxSize={4} />}
        <Text as="span" m={0} lineHeight="1">
          {currentState.buttonText}
        </Text>
      </Flex>
    );
  };

  return (
    <WithTooltip isDisabled={isDisabled} tooltipText={currentState.tooltip || ''}>
      <Button
        size="sm"
        variant="outline"
        isDisabled={isDisabled || isLoading}
        colorScheme={undefined}
        onClick={handleClick}
        minW={currentState.minWidth}
        color={currentState.color}
        borderColor={currentState.borderColor}
        _hover={{ bg: currentState.hoverBg }}
        _disabled={{
          color: currentState.color,
          borderColor: currentState.borderColor,
          opacity: isLoading ? 0.8 : 1,
          cursor: isLoading ? 'progress' : 'not-allowed',
        }}
      >
        {renderContent()}
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
  disabledReason: PropTypes.shape({
    key: PropTypes.string,
    tooltip: PropTypes.string,
    buttonText: PropTypes.string,
  }),
  onMigrate: PropTypes.func,
  onDelete: PropTypes.func,
};

export default DAGHistoryMigrateButton;
