import React from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import { Text } from '@chakra-ui/react';
import axios from 'axios';
import constants from '../constants';
import StarshipPage from '../component/StarshipPage';
import MigrateButton from '../component/MigrateButton';

const description = (
  <Text fontSize="xl">
    Environment Variables can be used to set Airflow Configurations, Connections,
    Variables, or as values directly accessed from a DAG or Task.
  </Text>
);
const columnHelper = createColumnHelper();
const buttonColumn = columnHelper.display({
  id: 'migrate',
  header: 'Migrate',
  cell: (props) => (
    <MigrateButton
      route={constants.ENV_VAR_ROUTE}
      data={{
        key: props.row.getValue('key'), value: props.row.getValue('value'),
      }}
    />
  ),
});

export default function EnvVarsPage() {
  const [data, setData] = React.useState([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState(null);
  React.useEffect(() => {
    axios.get(constants.ENV_VAR_ROUTE)
      .then((res) => {
        setData(Object.entries(res.data).map(([k, v]) => ({ key: k, value: v })));
        setLoading(false);
      })
      .catch((err) => {
        setError(err);
        setLoading(false);
      });
  }, []);

  const columns = [columnHelper.accessor('key'), columnHelper.accessor('value'), buttonColumn];
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
