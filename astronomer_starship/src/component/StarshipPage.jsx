import { Box, Divider } from '@chakra-ui/react';
import React from 'react';
import PropTypes from 'prop-types';
import PageLoading from './PageLoading';
import DataTable from './DataTable';

export default function StarshipPage({
  description, loading, error, data, columns,
}) {
  return (
    <Box>
      {description}
      <Divider my="3" />
      {loading || error ? <PageLoading loading={loading} error={error} /> : <DataTable data={data} columns={columns} />}
    </Box>
  );
}
StarshipPage.propTypes = {
  description: PropTypes.element,
  loading: PropTypes.bool,
  // eslint-disable-next-line react/forbid-prop-types
  error: PropTypes.object,
  // eslint-disable-next-line react/forbid-prop-types
  data: PropTypes.array,
  // eslint-disable-next-line react/forbid-prop-types
  columns: PropTypes.array.isRequired,
};
StarshipPage.defaultProps = {
  description: '',
  error: null,
  loading: false,
  data: [],
};
