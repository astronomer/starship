import React, { useMemo } from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import {
  Button, HStack, Text, Box, Heading, VStack, Stack,
} from '@chakra-ui/react';
import { RepeatIcon } from '@chakra-ui/icons';

import useMigrationData from '../hooks/useMigrationData';
import MigrateButton from '../component/MigrateButton';
import HiddenValue from '../component/HiddenValue';
import ProgressSummary from '../component/ProgressSummary';
import DataTable from '../component/DataTable';
import PageLoading from '../component/PageLoading';
import { objectWithoutKey, proxyHeaders, proxyUrl } from '../util';
import constants from '../constants';

const columnHelper = createColumnHelper();

/**
 * Renders a hidden value cell.
 */
function renderHiddenValue(info) {
  return <HiddenValue value={info.getValue()} />;
}

/**
 * Creates column definitions for the connections table.
 * Defined outside component to avoid unstable nested components.
 */
function createColumns(targetUrl, token, handleItemStatusChange) {
  return [
    columnHelper.accessor('conn_id', { header: 'Connection ID' }),
    columnHelper.accessor('conn_type', { header: 'Type' }),
    columnHelper.accessor('host', { header: 'Host' }),
    columnHelper.accessor('port', { header: 'Port' }),
    columnHelper.accessor('schema', { header: 'Schema' }),
    columnHelper.accessor('login', { header: 'Login' }),
    columnHelper.accessor('password', {
      header: 'Password',
      cell: renderHiddenValue,
      enableSorting: false,
    }),
    columnHelper.accessor('extra', {
      header: 'Extra',
      cell: renderHiddenValue,
      enableSorting: false,
    }),
    columnHelper.display({
      id: 'migrate',
      header: 'Migrate',
      meta: { align: 'right' },
      enableSorting: false,
      cell: (info) => {
        const { original } = info.row;
        return (
          <MigrateButton
            route={proxyUrl(targetUrl + constants.CONNECTIONS_ROUTE)}
            headers={proxyHeaders(token)}
            existsInRemote={original.exists}
            sendData={objectWithoutKey(original, 'exists')}
            onStatusChange={(newStatus) => handleItemStatusChange(original.conn_id, newStatus)}
            itemName={`connection "${original.conn_id}"`}
          />
        );
      },
    }),
  ];
}

export default function ConnectionsPage() {
  const {
    loading,
    error,
    data,
    isMigratingAll,
    totalItems,
    migratedItems,
    fetchData,
    handleItemStatusChange,
    handleMigrateAll,
    targetUrl,
    token,
  } = useMigrationData({
    route: constants.CONNECTIONS_ROUTE,
    idField: 'conn_id',
    itemName: 'connection',
  });

  const columns = useMemo(
    () => createColumns(targetUrl, token, handleItemStatusChange),
    [targetUrl, token, handleItemStatusChange],
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
          <Heading size="md" mb={0.5}>Connections</Heading>
          <Text fontSize="xs" color="gray.600">
            Airflow Connection objects store credentials for external services.
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
            <DataTable data={data} columns={columns} searchPlaceholder="Search by ID, type, host..." />
          )}
        </Box>
      </VStack>
    </Box>
  );
}
