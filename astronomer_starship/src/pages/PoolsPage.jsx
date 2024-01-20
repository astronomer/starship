import React, { useEffect, useState } from 'react';
import { Text } from '@chakra-ui/react';
import { createColumnHelper } from '@tanstack/react-table';
import PropTypes from 'prop-types';
import MigrateButton from '../component/MigrateButton';
import StarshipPage from '../component/StarshipPage';
import { fetchData, proxyHeaders, proxyUrl } from '../util';
import constants from '../constants';

const description = (
  <Text fontSize="xl">
    Pools are used to limit the number of concurrent tasks of a certain type that
    are running.
  </Text>
);
const columnHelper = createColumnHelper();

function setPoolsData(localData, remoteData) {
  return localData.map(
    (d) => ({
      ...d,
      exists: remoteData.map(
        // eslint-disable-next-line camelcase
        ({ name }) => name,
      ).includes(d.name),
    }),
  );
}

export default function PoolsPage({ state, dispatch }) {
  const [data, setData] = useState(setPoolsData(state.poolsLocalData, state.poolsRemoteData));
  useEffect(() => {
    fetchData(
      constants.POOL_ROUTE,
      state.targetUrl + constants.POOL_ROUTE,
      state.token,
      () => dispatch({ type: 'set-pools-loading' }),
      (res, rRes) => dispatch({
        type: 'set-pools-data', poolsLocalData: res.data, poolsRemoteData: rRes.data,
      }),
      (err) => dispatch({ type: 'set-pools-error', error: err }),
      dispatch,
    );
  }, []);
  useEffect(
    () => setData(setPoolsData(state.poolsLocalData, state.poolsRemoteData)),
    [state.poolsLocalData, state.poolsRemoteData],
  );

  // noinspection JSCheckFunctionSignatures
  const columns = [
    columnHelper.accessor('name'),
    columnHelper.accessor('slots'),
    columnHelper.accessor('description'),
    columnHelper.display({
      id: 'migrate',
      header: 'Migrate',
      // eslint-disable-next-line react/no-unstable-nested-components
      cell: (info) => (
        <MigrateButton
          route={proxyUrl(state.targetUrl + constants.POOL_ROUTE)}
          headers={proxyHeaders(state.token)}
          existsInRemote={info.row.original.exists}
          sendData={{
            name: info.row.getValue('name'),
            slots: info.row.getValue('slots'),
            description: info.row.getValue('description'),
          }}
        />
      ),
    }),
  ];

  return (
    <StarshipPage
      description={description}
      loading={state.poolsLoading}
      data={data}
      columns={columns}
      error={state.poolsError}
    />
  );
}
PoolsPage.propTypes = {
  // eslint-disable-next-line react/forbid-prop-types
  state: PropTypes.object.isRequired,
  dispatch: PropTypes.func.isRequired,
};
