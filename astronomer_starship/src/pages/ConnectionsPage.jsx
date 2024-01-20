import React, { useEffect, useState } from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import { Text } from '@chakra-ui/react';
import PropTypes from 'prop-types';
import StarshipPage from '../component/StarshipPage';
import MigrateButton from '../component/MigrateButton';
import HiddenValue from '../component/HiddenValue';
import { fetchData, proxyHeaders, proxyUrl } from '../util';
import constants from '../constants';

const description = (
  <Text fontSize="xl">
    Airflow Connection objects are used for storing credentials and other information
    necessary for connecting to external services.
    Connections can be defined via multiple mechanisms,
    Starship only migrates values stored via the Airflow UI.
  </Text>
);
const columnHelper = createColumnHelper();
const passwordColumn = columnHelper.accessor('password', {
  id: 'password', cell: (props) => <HiddenValue value={props.getValue()} />,
});
const extraColumn = columnHelper.accessor('extra', {
  id: 'extra', cell: (props) => <HiddenValue value={props.getValue()} />,
});

function setConnectionsData(localData, remoteData) {
  return localData.map(
    (d) => ({
      ...d,
      exists: remoteData.map(
        // eslint-disable-next-line camelcase
        ({ conn_id }) => conn_id,
      ).includes(d.conn_id),
    }),
  );
}

export default function ConnectionsPage({ state, dispatch }) {
  const [data, setData] = useState(
    setConnectionsData(state.connectionsLocalData, state.connectionsRemoteData),
  );
  useEffect(() => {
    fetchData(
      constants.CONNECTIONS_ROUTE,
      state.targetUrl + constants.CONNECTIONS_ROUTE,
      state.token,
      () => dispatch({ type: 'set-connections-loading' }),
      (res, rRes) => dispatch({
        type: 'set-connections-data', connectionsLocalData: res.data, connectionsRemoteData: rRes.data,
      }),
      (err) => dispatch({ type: 'set-connections-error', error: err }),
      dispatch,
    );
  }, []);
  useEffect(
    () => setData(setConnectionsData(state.connectionsLocalData, state.connectionsRemoteData)),
    [state.connectionsLocalData, state.connectionsRemoteData],
  );

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
    columnHelper.display({
      id: 'migrate',
      header: 'Migrate',
      // eslint-disable-next-line react/no-unstable-nested-components
      cell: (info) => (
        <MigrateButton
          route={proxyUrl(state.targetUrl + constants.CONNECTIONS_ROUTE)}
          headers={proxyHeaders(state.token)}
          existsInRemote={info.row.original.exists}
          sendData={{
            conn_id: info.row.getValue('conn_id'),
            conn_type: info.row.getValue('conn_type'),
            host: info.row.getValue('host'),
            port: info.row.getValue('port'),
            schema: info.row.getValue('schema'),
            login: info.row.getValue('login'),
            password: info.row.getValue('password'),
            extra: info.row.getValue('extra'),
          }}
        />
      ),
    }),
  ];
  return (
    <StarshipPage
      description={description}
      loading={state.connectionsLoading}
      data={data}
      columns={columns}
      error={state.error}
    />
  );
}
ConnectionsPage.propTypes = {
  // eslint-disable-next-line react/forbid-prop-types
  state: PropTypes.object.isRequired,
  dispatch: PropTypes.func.isRequired,
};
