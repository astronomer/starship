import React from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import axios from 'axios';
import { Text } from '@chakra-ui/react';
import constants from '../constants';
import MigrateButton from '../component/MigrateButton';
import StarshipPage from '../component/StarshipPage';

const description = (
  <Text fontSize="xl">
    Variables are a generic way to store and retrieve arbitrary content or settings
    as a simple key value store within Airflow. Variables can be defined via multiple mechanisms,
    Starship only migrates values stored via the Airflow UI.
  </Text>
);
const columnHelper = createColumnHelper();
const buttonColumn = columnHelper.display({
  id: 'migrate',
  header: 'Migrate',
  cell: (props) => (
    <MigrateButton
      route={constants.VARIABLES_ROUTE}
      data={{ key: props.row.getValue('key'), val: props.row.getValue('val') }}
    />
  ),
});

export default function VariablesPage() {
  const [data, setData] = React.useState([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState(null);
  React.useEffect(() => {
    axios.get(constants.VARIABLES_ROUTE)
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
    columnHelper.accessor('key'),
    columnHelper.accessor('val'),
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
