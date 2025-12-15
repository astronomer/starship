import React, { useEffect, useState, useCallback } from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import {
  Button, HStack, Text, useToast, Box, Heading, VStack, Stack,
} from '@chakra-ui/react';
import { RepeatIcon } from '@chakra-ui/icons';
import axios from 'axios';

import { useAppDispatch, useTargetConfig } from '../AppContext';
import MigrateButton from '../component/MigrateButton';
import ProgressSummary from '../component/ProgressSummary';
import DataTable from '../component/DataTable';
import PageLoading from '../component/PageLoading';
import {
  localRoute, objectWithoutKey, proxyHeaders, proxyUrl,
} from '../util';
import constants from '../constants';

const columnHelper = createColumnHelper();

function mergeData(localData, remoteData) {
  return localData.map((item) => ({
    ...item,
    exists: remoteData.some((remote) => remote.name === item.name),
  }));
}

export default function PoolsPage() {
  const { targetUrl, token } = useTargetConfig();
  const dispatch = useAppDispatch();
  const toast = useToast();

  // Local state for this page only
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState([]);
  const [isMigratingAll, setIsMigratingAll] = useState(false);

  // Fetch data on mount
  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const [localRes, remoteRes] = await Promise.all([
        axios.get(localRoute(constants.POOL_ROUTE)),
        axios.get(proxyUrl(targetUrl + constants.POOL_ROUTE), {
          headers: proxyHeaders(token),
        }),
      ]);

      if (localRes.status === 200 && remoteRes.status === 200) {
        setData(mergeData(localRes.data, remoteRes.data));
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

  // Handle individual item status change
  const handleItemStatusChange = useCallback((name, newStatus) => {
    setData((prev) => prev.map((item) => (
      item.name === name ? { ...item, exists: newStatus } : item
    )));
  }, []);

  // Migrate all unmigrated items
  const handleMigrateAll = useCallback(async () => {
    const unmigratedItems = data.filter((item) => !item.exists);
    if (unmigratedItems.length === 0) return;

    setIsMigratingAll(true);
    let successCount = 0;
    let errorCount = 0;

    for (const item of unmigratedItems) {
      try {
        await axios.post(
          proxyUrl(targetUrl + constants.POOL_ROUTE),
          objectWithoutKey(item, 'exists'),
          { headers: proxyHeaders(token) },
        );
        successCount += 1;
        setData((prev) => prev.map((d) => (
          d.name === item.name ? { ...d, exists: true } : d
        )));
      } catch (err) {
        errorCount += 1;
      }
    }

    setIsMigratingAll(false);

    toast({
      title: successCount > 0
        ? `Successfully migrated ${successCount} pool${successCount !== 1 ? 's' : ''}`
        : 'Migration failed',
      description: errorCount > 0 ? `${errorCount} item${errorCount !== 1 ? 's' : ''} failed` : undefined,
      status: successCount > 0 ? (errorCount > 0 ? 'warning' : 'success') : 'error',
      duration: 5000,
      isClosable: true,
    });
  }, [data, targetUrl, token, toast]);

  // Define columns
  const columns = React.useMemo(() => [
    columnHelper.accessor('name', { header: 'Name' }),
    columnHelper.accessor('slots', { header: 'Slots' }),
    columnHelper.accessor('description', { header: 'Description' }),
    columnHelper.display({
      id: 'migrate',
      header: 'Migrate',
      meta: { align: 'right' },
      cell: ({ row }) => (
        <MigrateButton
          route={proxyUrl(targetUrl + constants.POOL_ROUTE)}
          headers={proxyHeaders(token)}
          existsInRemote={row.original.exists}
          sendData={objectWithoutKey(row.original, 'exists')}
          onStatusChange={(newStatus) => handleItemStatusChange(row.original.name, newStatus)}
        />
      ),
    }),
  ], [targetUrl, token, handleItemStatusChange]);

  // Calculate progress
  const totalItems = data.length;
  const migratedItems = data.filter((item) => item.exists).length;

  return (
    <Box>
      <Stack
        direction={{ base: 'column', md: 'row' }}
        justify="space-between"
        align={{ base: 'flex-start', md: 'center' }}
        mb={3}
      >
        <Box>
          <Heading size="md" mb={0.5}>Pools</Heading>
          <Text fontSize="xs" color="gray.600">
            Pools limit the number of concurrent tasks of a certain type.
          </Text>
        </Box>
        <HStack>
          <Button
            size="sm"
            leftIcon={<RepeatIcon />}
            onClick={fetchData}
            variant="outline"
            isLoading={loading}
          >
            Refresh
          </Button>
        </HStack>
      </Stack>

      <VStack spacing={3} align="stretch" w="100%">
        {!loading && !error && (
          <ProgressSummary
            totalItems={totalItems}
            migratedItems={migratedItems}
            onMigrateAll={handleMigrateAll}
            isMigratingAll={isMigratingAll}
          />
        )}

        <Box>
          {loading || error ? (
            <PageLoading loading={loading} error={error} />
          ) : (
            <DataTable data={data} columns={columns} searchPlaceholder="Search pools..." />
          )}
        </Box>
      </VStack>
    </Box>
  );
}
