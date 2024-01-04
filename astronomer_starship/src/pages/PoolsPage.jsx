import React from 'react';
import axios from 'axios';
import { Text } from '@chakra-ui/react';
import { createColumnHelper } from '@tanstack/react-table';
import constants from '../constants';
import MigrateButton from '../component/MigrateButton';
import StarshipPage from '../component/StarshipPage';

const description = (
  <Text fontSize="xl">
    Pools are used to limit the number of concurrent tasks of a certain type that
    are running.
  </Text>
);
const columnHelper = createColumnHelper();
const buttonColumn = columnHelper.display({
  id: 'migrate',
  header: 'Migrate',
  cell: (props) => (
    <MigrateButton
      route={constants.POOL_ROUTE}
      data={{
        name: props.row.getValue('name'),
        slots: props.row.getValue('slots'),
        description: props.row.getValue('description'),
      }}
    />
  ),
});

export default function PoolsPage() {
  const [data, setData] = React.useState([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState(null);
  React.useEffect(() => {
    axios.get(constants.POOL_ROUTE)
      .then((res) => {
        setData(res.data);
        setLoading(false);
      })
      .catch((err) => {
        setError(err);
        setLoading(false);
      });
  }, []);

  // noinspection JSCheckFunctionSignatures
  const columns = [
    columnHelper.accessor('name'),
    columnHelper.accessor('slots'),
    columnHelper.accessor('description'),
    buttonColumn,
  ];

  return (
    <StarshipPage
      description={description}
      loading={loading}
      data={data}
      columns={columns}
      error={error}
    />
  );
}
