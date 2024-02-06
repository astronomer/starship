/* eslint-disable no-nested-ternary */
import React, { useEffect, useState } from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import {
  Text, Button, useToast, Tooltip, HStack, Spacer,
} from '@chakra-ui/react';
import PropTypes from 'prop-types';
import axios from 'axios';
import { MdErrorOutline } from 'react-icons/md';
import { FaCheck } from 'react-icons/fa';
import { GoUpload } from 'react-icons/go';
import { RepeatIcon } from '@chakra-ui/icons';
import StarshipPage from '../component/StarshipPage';
import MigrateButton from '../component/MigrateButton';
import { fetchData, proxyHeaders, proxyUrl } from '../util';
import constants from '../constants';

// noinspection JSUnusedLocalSymbols
export function MigrateEnvButton({
  // eslint-disable-next-line no-unused-vars,react/prop-types
  isAstro, route, headers, existsInRemote, sendData,
}) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const toast = useToast();
  const [exists, setExists] = useState(existsInRemote);

  function handleClick() {
    setLoading(true);
    axios.post(route, sendData, { headers })
      .then((res) => {
        setLoading(false);
        setExists(res.status === 200);
      })
      .catch((err) => {
        setExists(false);
        setLoading(false);
        toast({
          title: err.response?.data?.error || err.response?.data || err.message,
          status: 'error',
          isClosable: true,
        });
        setError(err);
      });
  }

  return (
    <Tooltip hasArrow label="Not Yet Implemented">
      <Button
        isDisabled={loading || exists}
        isLoading={loading}
        loadingText="Loading"
        variant="solid"
        leftIcon={(
          error ? <MdErrorOutline /> : exists ? <FaCheck /> : !loading ? <GoUpload /> : <span />
        )}
        colorScheme={
          exists ? 'green' : loading ? 'teal' : error ? 'red' : 'teal'
        }
        onClick={() => handleClick()}
      >
        {exists ? 'Ok' : loading ? '' : error ? 'Error!' : 'Migrate'}
      </Button>
    </Tooltip>
  );
}

MigrateEnvButton.propTypes = {
  route: PropTypes.string.isRequired,
  headers: PropTypes.objectOf(PropTypes.string),
  existsInRemote: PropTypes.bool,
  // eslint-disable-next-line react/forbid-prop-types
  sendData: PropTypes.object.isRequired,
};
MigrateEnvButton.defaultProps = {
  headers: {},
  existsInRemote: false,
};

const columnHelper = createColumnHelper();

function setEnvData(localData, remoteData) {
  return Object.entries(localData).map(
    ([key, value]) => ({ key, value }),
  ).map((d) => ({
    ...d,
    exists: d.key in remoteData,
  }));
}

export default function EnvVarsPage({ state, dispatch }) {
  const [data, setData] = useState(setEnvData(state.envLocalData, state.envRemoteData));
  const fetchPageData = () => fetchData(
    constants.ENV_VAR_ROUTE,
    state.targetUrl + constants.ENV_VAR_ROUTE,
    state.token,
    () => dispatch({ type: 'set-env-loading' }),
    (res, rRes) => dispatch({
      type: 'set-env-data', envLocalData: res.data, envRemoteData: rRes.data,
    }),
    (err) => dispatch({ type: 'set-env-error', error: err }),
  );
  useEffect(() => fetchPageData(), []);
  useEffect(
    () => setData(setEnvData(state.envLocalData, state.envRemoteData)),
    [state],
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
          isDisabled
          isAstro={state.isAstro}
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
      description={(
        <HStack>
          <Text fontSize="xl">
            Environment Variables can be used to set Airflow Configurations, Connections,
            Variables, or as values directly accessed from a DAG or Task.
          </Text>
          <Spacer />
          <Button size="sm" leftIcon={<RepeatIcon />} onClick={() => fetchPageData()}>Reset</Button>
        </HStack>
      )}
      loading={state.envLoading}
      data={data}
      columns={columns}
      error={state.envError}
      resetFn={fetchPageData}
    />
  );
}
EnvVarsPage.propTypes = {
  // eslint-disable-next-line react/forbid-prop-types
  state: PropTypes.object.isRequired,
  dispatch: PropTypes.func.isRequired,
};
