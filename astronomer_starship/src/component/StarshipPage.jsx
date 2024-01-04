import {
  Box, Divider, VStack,
} from '@chakra-ui/react';
import React from 'react';
import PropTypes from 'prop-types';
import PageLoading from './PageLoading';
import ErrorMessage from './ErrorMessage';
import DataTable from './DataTable';

export default function StarshipPage({
  description, loading, error, data, columns,
}) {
  return (
    <VStack>
      <Box width="100%" margin="30px">
        {description}
      </Box>
      <Divider />
      {loading ? <PageLoading loading={loading} /> : null}
      {error ? <ErrorMessage error={error} /> : null}
      {!error && !loading ? (
        <DataTable
          data={data}
          columns={columns}
        />
      ) : null}
    </VStack>
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
