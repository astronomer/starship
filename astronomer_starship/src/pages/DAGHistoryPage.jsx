import React from 'react';
import axios from 'axios';
import { createColumnHelper } from '@tanstack/react-table';
import { Text } from '@chakra-ui/react';
import constants from '../constants';
import MigrateButton from '../component/MigrateButton';
import StarshipPage from '../component/StarshipPage';

const description = (
  <Text fontSize="xl">
    DAGs and Task History can be migrated to prevent Airflow from re-doing old runs.
    DAGs can be paused or unpaused on either Airflow instance.
  </Text>
);
const columnHelper = createColumnHelper();
const buttonColumn = columnHelper.display({
  id: 'migrate',
  header: 'Migrate',
  cell: (props) => (
    <MigrateButton
      route={constants.DAG_HISTORY_ROUTE}
      data={{
        name: props.row.getValue('name'),
      }}
    />
  ),
});

export default function DAGHistoryPage() {
  const [data, setData] = React.useState([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState(null);
  // TODO - implement
  React.useEffect(() => {
    axios.get(constants.DAG_HISTORY_ROUTE)
      .then((res) => {
        setData(res.data);
        setLoading(false);
      })
      .catch((err) => {
        setError(err);
        setLoading(false);
      });
  }, []);

  // TODO - implement
  // noinspection JSCheckFunctionSignatures
  const columns = [columnHelper.accessor('name'), buttonColumn];

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
