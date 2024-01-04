import React from 'react';
import axios from 'axios';
import { createColumnHelper } from '@tanstack/react-table';
import { Text } from '@chakra-ui/react';
import constants from '../constants';
import StarshipPage from '../component/StarshipPage';
import MigrateButton from '../component/MigrateButton';
import HiddenValue from '../component/HiddenValue';

const description = (
  <Text fontSize="xl">
    Airflow Connection objects are used for storing credentials and other information
    necessary for connecting to external services. Variables can be defined via multiple mechanisms,
    Starship only migrates values stored via the Airflow UI.
  </Text>
);
const columnHelper = createColumnHelper();
const buttonColumn = columnHelper.display({
  id: 'migrate',
  header: 'Migrate',
  cell: (props) => (
    <MigrateButton
      route={constants.CONNECTIONS_ROUTE}
      data={{
        conn_id: props.row.getValue('conn_id'),
        conn_type: props.row.getValue('conn_type'),
        host: props.row.getValue('host'),
        port: props.row.getValue('port'),
        schema: props.row.getValue('schema'),
        login: props.row.getValue('login'),
        password: props.row.getValue('password'),
        extra: props.row.getValue('extra'),
      }}
    />
  ),
});
const passwordColumn = columnHelper.accessor('password', {
  id: 'password', cell: (props) => <HiddenValue value={props.getValue()} />,
});
const extraColumn = columnHelper.accessor('extra', {
  id: 'extra', cell: (props) => <HiddenValue value={props.getValue()} />,
});

export default function ConnectionsPage() {
  const [data, setData] = React.useState([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState(null);
  React.useEffect(() => {
    axios.get(constants.CONNECTIONS_ROUTE)
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
    columnHelper.accessor('conn_id'),
    columnHelper.accessor('conn_type'),
    columnHelper.accessor('host'),
    columnHelper.accessor('port'),
    columnHelper.accessor('schema'),
    columnHelper.accessor('login'),
    passwordColumn,
    extraColumn,
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
