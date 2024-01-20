import React, { useEffect, useState } from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import { Text } from '@chakra-ui/react';
import PropTypes from 'prop-types';
import StarshipPage from '../component/StarshipPage';
import MigrateButton from '../component/MigrateButton';
import { fetchData, proxyHeaders, proxyUrl } from '../util';
import constants from '../constants';

const description = (
  <Text fontSize="xl">
    Environment Variables can be used to set Airflow Configurations, Connections,
    Variables, or as values directly accessed from a DAG or Task.
  </Text>
);
const columnHelper = createColumnHelper();

function setEnvData(localData, remoteData) {
  return Object.entries(localData).map(
    ([key, value]) => ({ key, value }),
  ).map(
    (d) => ({
      ...d,
      exists: Object.entries(remoteData).map(
        // eslint-disable-next-line camelcase
        ({ key }) => key,
      ).includes(d.key),
    }),
  );
}

export default function EnvVarsPage({ state, dispatch }) {
  const [data, setData] = useState(setEnvData(state.envLocalData, state.envRemoteData));
  useEffect(() => {
    fetchData(
      constants.ENV_VAR_ROUTE,
      state.targetUrl + constants.ENV_VAR_ROUTE,
      state.token,
      () => dispatch({ type: 'set-env-loading' }),
      (res, rRes) => dispatch({
        type: 'set-env-data', envLocalData: res.data, envRemoteData: rRes.data,
      }),
      (err) => dispatch({ type: 'set-env-error', error: err }),
      dispatch,
    );
  }, []);
  useEffect(
    () => setData(setEnvData(state.envLocalData, state.envRemoteData)),
    [state.envLocalData, state.envRemoteData],
  );

  // noinspection JSCheckFunctionSignatures
  const columns = [
    columnHelper.accessor('key'),
    columnHelper.accessor('value'),
    columnHelper.display({
      id: 'migrate',
      header: 'Migrate',
      // eslint-disable-next-line react/no-unstable-nested-components
      cell: (info) => (
        <MigrateButton
          route={proxyUrl(state.targetUrl + constants.ENV_VAR_ROUTE)}
          headers={proxyHeaders(state.token)}
          existsInRemote={info.row.original.exists}
          sendData={{
            key: info.row.getValue('key'),
            value: info.row.getValue('value'),
          }}
        />
      ),
    }),
  ];

  return (
    <StarshipPage
      description={description}
      loading={state.envLoading}
      data={data}
      columns={columns}
      error={state.envError}
    />
  );
}
EnvVarsPage.propTypes = {
  // eslint-disable-next-line react/forbid-prop-types
  state: PropTypes.object.isRequired,
  dispatch: PropTypes.func.isRequired,
};
