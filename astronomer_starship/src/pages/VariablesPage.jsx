import React, { useMemo } from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import {
  Button, HStack, Text, Box, Heading, VStack, Stack,
} from '@chakra-ui/react';
import { RepeatIcon } from '@chakra-ui/icons';

import useMigrationData from '../hooks/useMigrationData';
import MigrateButton from '../component/MigrateButton';
import ProgressSummary from '../component/ProgressSummary';
import DataTable from '../component/DataTable';
import PageLoading from '../component/PageLoading';
import { objectWithoutKey, proxyHeaders, proxyUrl } from '../util';
import constants from '../constants';

const columnHelper = createColumnHelper();

/**
 * Creates column definitions for the variables table.
 * Defined outside component to avoid unstable nested components.
 */
function createColumns(targetUrl, token, handleItemStatusChange) {
  return [
    columnHelper.accessor('key', { header: 'Key' }),
    columnHelper.accessor('val', { header: 'Value' }),
    columnHelper.display({
      id: 'migrate',
      header: 'Migrate',
      meta: { align: 'right' },
      cell: (info) => {
        const { original } = info.row;
        return (
          <MigrateButton
            route={proxyUrl(targetUrl + constants.VARIABLES_ROUTE)}
            headers={proxyHeaders(token)}
            existsInRemote={original.exists}
            sendData={objectWithoutKey(original, 'exists')}
            onStatusChange={(newStatus) => handleItemStatusChange(original.key, newStatus)}
            itemName={`variable "${original.key}"`}
          />
        );
      },
    }),
  ];
}

export default function VariablesPage() {
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
    route: constants.VARIABLES_ROUTE,
    idField: 'key',
    itemName: 'variable',
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
          <Heading size="md" mb={0.5}>Variables</Heading>
          <Text fontSize="xs" color="gray.600">
            Variables store arbitrary content as key-value pairs.
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
            <DataTable data={data} columns={columns} searchPlaceholder="Search variables..." />
          )}
        </Box>
      </VStack>
    </Box>
  );
}
