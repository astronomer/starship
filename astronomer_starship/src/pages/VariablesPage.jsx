import React, { useEffect, useState } from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import {
  Button, HStack, Spacer, Text,
} from '@chakra-ui/react';
import PropTypes from 'prop-types';
import { RepeatIcon } from '@chakra-ui/icons';
import MigrateButton from '../component/MigrateButton';
import StarshipPage from '../component/StarshipPage';
import {
  fetchData, localRoute, objectWithoutKey, proxyHeaders, proxyUrl, remoteRoute,
} from '../util';
import constants from '../constants';

const columnHelper = createColumnHelper();

function setVariablesData(localData, remoteData) {
  return localData.map(
    (d) => ({
      ...d,
      exists: remoteData.map(
        ({ key }) => key,
      ).includes(d.key),
    }),
  );
}

export default function VariablesPage({ state, dispatch }) {
  const [data, setData] = useState(
    setVariablesData(state.variablesLocalData, state.variablesRemoteData),
  );
  const fetchPageData = () => fetchData(
    localRoute(constants.VARIABLES_ROUTE),
    remoteRoute(state.targetUrl, constants.VARIABLES_ROUTE),
    state.token,
    () => dispatch({ type: 'set-variables-loading' }),
    (res, rRes) => dispatch({
      type: 'set-variables-data', variablesLocalData: res.data, variablesRemoteData: rRes.data,
    }),
    (err) => dispatch({ type: 'set-variables-error', error: err }),
  );
  useEffect(() => fetchPageData(), []);
  useEffect(
    () => setData(setVariablesData(state.variablesLocalData, state.variablesRemoteData)),
    [state],
  );

  // noinspection JSCheckFunctionSignatures
  const columns = [
    columnHelper.accessor('key'),
    columnHelper.accessor('val'),
    // columnHelper.accessor('exists'),
    columnHelper.display({
      id: 'migrate',
      header: 'Migrate',
      // eslint-disable-next-line react/no-unstable-nested-components
      cell: (info) => (
        <MigrateButton
          route={proxyUrl(state.targetUrl + constants.VARIABLES_ROUTE)}
          headers={proxyHeaders(state.token)}
          existsInRemote={info.row.original.exists}
          sendData={{ ...objectWithoutKey(info.row.original, 'exists') }}
        />
      ),
    }),
  ];
  return (
    <StarshipPage
      description={(
        <HStack>
          <Text fontSize="xl">
            Variables are a generic way to store and retrieve arbitrary content or settings
            as a simple key value store within Airflow.
            Variables can be defined via multiple mechanisms,
            Starship only migrates values stored via the Airflow UI.
          </Text>
          <Spacer />
          <Button size="sm" leftIcon={<RepeatIcon />} onClick={() => fetchPageData()}>Reset</Button>
        </HStack>
      )}
      loading={state.variablesLoading}
      data={data}
      columns={columns}
      error={state.error}
      resetFn={fetchPageData}
    />
  );
}
VariablesPage.propTypes = {
  // eslint-disable-next-line react/forbid-prop-types
  state: PropTypes.object.isRequired,
  dispatch: PropTypes.func.isRequired,
};
