/* eslint-disable no-nested-ternary */
import React, { useState } from 'react';
import {
  Button,
  CircularProgress,
  HStack,
  Text,
  useToast,
} from '@chakra-ui/react';
import PropTypes from 'prop-types';
import axios from 'axios';
import { MdErrorOutline, MdDeleteForever, MdWarning } from 'react-icons/md';
import { GoUpload } from 'react-icons/go';

import WithTooltip from './WithTooltip';
import { localRoute, proxyHeaders, proxyUrl } from '../util';
import constants from '../constants';

/**
 * Migrate button specifically for DAG history migration.
 * Handles batch migration of DAG runs, task instances, and task instance history.
 */
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
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState(null);
  const toast = useToast();
  const [exists, setExists] = useState(existsInRemote);
  const [isDeleting, setIsDeleting] = useState(false);

  const isLoading = progress > 0 && progress < 100;

  const handleError = (err, action = 'migrate') => {
    setExists(false);
    setProgress(0);
    setIsDeleting(false);
    toast({
      title: `Failed to ${action} DAG history for "${dagId}"`,
      description: err.response?.data?.error || err.response?.data || err.message,
      status: 'error',
      isClosable: true,
      variant: 'outline',
      duration: 6000,
    });
    setError(err);
  };

  const handleClick = async () => {
    if (exists) {
      // Delete
      setIsDeleting(true);
      setProgress(50);
      try {
        await axios({
          method: 'delete',
          url: proxyUrl(url + constants.DAG_RUNS_ROUTE),
          headers: proxyHeaders(token),
          params: { dag_id: dagId },
        });
        // Delete succeeded - set exists to false regardless of status code
        setExists(false);
        onDelete?.(dagId);
        setProgress(100);
        setTimeout(() => {
          setProgress(0);
          setIsDeleting(false);
        }, 500);
      } catch (err) {
        handleError(err, 'delete');
        setIsDeleting(false);
      }
      return;
    }

    // Migrate
    setProgress(1);

    const migrateBatch = async (offset = 0, totalCount = null) => {
      const appliedBatchSize = Math.min(limit - offset, batchSize);
      try {
        const [dagRunsRes, taskInstanceRes, taskInstanceHistoryRes] = await Promise.all([
          axios.get(localRoute(constants.DAG_RUNS_ROUTE), {
            params: { dag_id: dagId, limit: appliedBatchSize, offset },
          }),
          axios.get(localRoute(constants.TASK_INSTANCE_ROUTE), {
            params: { dag_id: dagId, limit: appliedBatchSize, offset },
          }),
          axios.get(localRoute(constants.TASK_INSTANCE_HISTORY_ROUTE), {
            params: { dag_id: dagId, limit: appliedBatchSize, offset },
          }),
        ]);

        const dagRunsToMigrateCount = totalCount
          || Math.min(dagRunsRes.data.dag_run_count, limit);

        if (dagRunsRes.data.dag_runs.length === 0) {
          setProgress(100);
          setExists(offset > 0);
          onMigrate?.(dagId, dagRunsToMigrateCount);
          setTimeout(() => setProgress(0), 500);
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

        const taskInstanceCreateRes = await axios.post(
          proxyUrl(url + constants.TASK_INSTANCE_ROUTE),
          { task_instances: taskInstanceRes.data.task_instances },
          { params: { dag_id: dagId }, headers: proxyHeaders(token) },
        );

        if (taskInstanceCreateRes.status !== 200) {
          throw new Error('Failed to create task instances');
        }

        const taskInstanceHistoryCreateRes = await axios.post(
          proxyUrl(url + constants.TASK_INSTANCE_HISTORY_ROUTE),
          { task_instances: taskInstanceHistoryRes.data.task_instances },
          { params: { dag_id: dagId }, headers: proxyHeaders(token) },
        );

        if (taskInstanceHistoryCreateRes.status !== 200) {
          throw new Error('Failed to create task instance history');
        }

        const migratedCount = dagRunCreateRes.data.dag_run_count;

        if (migratedCount < dagRunsToMigrateCount) {
          const newProgress = Math.min(
            99,
            Math.round((migratedCount / dagRunsToMigrateCount) * 100),
          );
          setProgress(newProgress);
          await migrateBatch(offset + batchSize, dagRunsToMigrateCount);
        } else {
          setProgress(100);
          setExists(true);
          onMigrate?.(dagId, dagRunsToMigrateCount);
          setTimeout(() => setProgress(0), 500);
        }
      } catch (err) {
        handleError(err);
      }
    };

    await migrateBatch(0);
  };

  // Render progress indicator or button content
  const renderContent = () => {
    if (isLoading) {
      return (
        <HStack spacing={2}>
          <CircularProgress
            value={progress}
            size="20px"
            thickness="10px"
            color={isDeleting ? 'red.400' : 'green.400'}
            trackColor="gray.200"
            isIndeterminate={progress < 2}
          />
          <Text fontSize="xs">
            {isDeleting ? 'Deleting' : 'Migrating'}
            {' '}
            {Math.round(progress)}
            %
          </Text>
        </HStack>
      );
    }
    if (exists) return 'Delete';
    if (isDisabled) return 'Not Found';
    if (error) return 'Error!';
    return 'Migrate';
  };

  // Determine colors based on state
  const isDeleteMode = exists || isDeleting;
  const activeColor = isDeleteMode || error ? 'error.600' : 'success.600';
  const activeBorderColor = isDeleteMode || error ? 'error.500' : 'success.500';
  const activeHoverBg = isDeleteMode || error ? 'error.50' : 'success.50';

  return (
    <WithTooltip isDisabled={isDisabled}>
      <Button
        size="sm"
        variant="outline"
        isDisabled={isDisabled || isLoading}
        leftIcon={
          isLoading ? null
            : error ? <MdErrorOutline />
              : exists ? <MdDeleteForever />
                : isDisabled ? <MdWarning />
                  : <GoUpload />
        }
        colorScheme={undefined}
        color={activeColor}
        borderColor={activeBorderColor}
        _hover={{
          bg: activeHoverBg,
        }}
        _disabled={isLoading ? {
          color: activeColor,
          borderColor: activeBorderColor,
          opacity: 0.8,
          cursor: 'progress',
        } : {
          color: 'gray.600',
          borderColor: 'gray.500',
          opacity: 1,
        }}
        onClick={handleClick}
        minW={isLoading ? '130px' : undefined}
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
  isDisabled: PropTypes.oneOfType([PropTypes.bool, PropTypes.string]),
  onMigrate: PropTypes.func,
  onDelete: PropTypes.func,
};

export default DAGHistoryMigrateButton;
