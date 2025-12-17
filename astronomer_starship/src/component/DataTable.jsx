import React, { useState } from 'react';
import {
  Box,
  HStack,
  Input,
  InputGroup,
  InputLeftElement,
  Table,
  TableContainer,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  chakra,
} from '@chakra-ui/react';
import { SearchIcon, TriangleDownIcon, TriangleUpIcon } from '@chakra-ui/icons';
import {
  useReactTable,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  getFilteredRowModel,
} from '@tanstack/react-table';
import PropTypes from 'prop-types';

/**
 * Global filter function for searching across all columns
 */
function globalFilterFn(row, columnId, filterValue) {
  const search = filterValue.toLowerCase();
  const value = row.getValue(columnId);

  if (value == null) return false;

  // Handle arrays (like tags)
  if (Array.isArray(value)) {
    return value.some((item) => String(item).toLowerCase().includes(search));
  }

  // Handle objects
  if (typeof value === 'object') {
    return JSON.stringify(value).toLowerCase().includes(search);
  }

  return String(value).toLowerCase().includes(search);
}

export default function DataTable({
  data,
  columns,
  searchPlaceholder = 'Search...',
  showSearch = true,
  rightElement = null,
}) {
  const [sorting, setSorting] = useState([]);
  const [globalFilter, setGlobalFilter] = useState('');

  const table = useReactTable({
    columns,
    data,
    getCoreRowModel: getCoreRowModel(),
    onSortingChange: setSorting,
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    onGlobalFilterChange: setGlobalFilter,
    globalFilterFn,
    state: { sorting, globalFilter },
  });

  const rowCount = table.getRowModel().rows.length;
  const totalCount = data.length;

  return (
    <Box>
      {showSearch && (
        <HStack mb={3} justify="space-between" align="center">
          <HStack spacing={3} flex={1}>
            <InputGroup size="sm" maxW="xs">
              <InputLeftElement pointerEvents="none">
                <SearchIcon color="gray.400" />
              </InputLeftElement>
              <Input
                placeholder={searchPlaceholder}
                value={globalFilter}
                onChange={(e) => setGlobalFilter(e.target.value)}
                borderRadius="md"
                focusBorderColor="brand.400"
              />
            </InputGroup>
            {globalFilter && (
              <Text fontSize="sm" color="gray.500">
                {rowCount}
                {' '}
                of
                {totalCount}
                {' '}
                items
              </Text>
            )}
          </HStack>
          {rightElement}
        </HStack>
      )}
      <TableContainer className="data-table">
        <Table variant="striped" size="sm">
          <Thead>
            {table.getHeaderGroups().map((headerGroup) => (
              <Tr key={headerGroup.id}>
                {headerGroup.headers.map((header) => {
                  const { meta } = header.column.columnDef;
                  const canSort = header.column.getCanSort();
                  return (
                    <Th
                      key={header.id}
                      onClick={canSort ? header.column.getToggleSortingHandler() : undefined}
                      isNumeric={meta?.isNumeric}
                      textAlign={meta?.align || (meta?.isNumeric ? 'right' : 'left')}
                      width={meta?.width || 'auto'}
                      cursor={canSort ? 'pointer' : 'default'}
                    >
                      {header.isPlaceholder
                        ? null
                        : flexRender(header.column.columnDef.header, header.getContext())}
                      <chakra.span pl="4">
                        {header.column.getIsSorted() === 'desc' && (
                          <TriangleDownIcon aria-label="sorted descending" />
                        )}
                        {header.column.getIsSorted() === 'asc' && (
                          <TriangleUpIcon aria-label="sorted ascending" />
                        )}
                      </chakra.span>
                    </Th>
                  );
                })}
              </Tr>
            ))}
          </Thead>
          <Tbody>
            {table.getRowModel().rows.length === 0 ? (
              <Tr>
                <Td colSpan={columns.length} textAlign="center" py={8} color="gray.500">
                  {globalFilter ? 'No matching results' : 'No data available'}
                </Td>
              </Tr>
            ) : (
              table.getRowModel().rows.map((row) => (
                <Tr key={row.id}>
                  {row.getVisibleCells().map((cell) => {
                    const { meta } = cell.column.columnDef;
                    return (
                      <Td
                        key={cell.id}
                        isNumeric={meta?.isNumeric}
                        textAlign={meta?.align || (meta?.isNumeric ? 'right' : 'left')}
                        width={meta?.width || 'auto'}
                      >
                        {flexRender(cell.column.columnDef.cell, cell.getContext())}
                      </Td>
                    );
                  })}
                </Tr>
              ))
            )}
          </Tbody>
        </Table>
      </TableContainer>
    </Box>
  );
}

DataTable.propTypes = {
  // eslint-disable-next-line react/forbid-prop-types
  data: PropTypes.arrayOf(PropTypes.object).isRequired,
  // eslint-disable-next-line react/forbid-prop-types
  columns: PropTypes.arrayOf(PropTypes.object).isRequired,
  searchPlaceholder: PropTypes.string,
  showSearch: PropTypes.bool,
  rightElement: PropTypes.node,
};
