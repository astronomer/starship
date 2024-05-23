import React, { useEffect, useState } from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import {
  Text, Button, useToast, HStack, Spacer,
} from '@chakra-ui/react';
import PropTypes from 'prop-types';
import axios from 'axios';
import { MdErrorOutline } from 'react-icons/md';
import { FaCheck } from 'react-icons/fa';
import { GoUpload } from 'react-icons/go';
import { RepeatIcon } from '@chakra-ui/icons';
import StarshipPage from '../component/StarshipPage';
import {
  fetchData, getAstroEnvVarRoute, getHoustonRoute, localRoute, proxyHeaders, proxyUrl, remoteRoute,
} from '../util';
import constants from '../constants';
import HiddenValue from "../component/HiddenValue.jsx";

const getDeploymentsQuery = `query deploymentVariables($deploymentUuid: Uuid!, $releaseName: String!) {
  deploymentVariables(
    deploymentUuid: $deploymentUuid
    releaseName: $releaseName
  ) {
    key
    value
    isSecret
  }
}`;

const updateDeploymentVariablesMutation = `
mutation UpdateDeploymentVariables(
  $deploymentUuid:Uuid!,
  $releaseName:String!,
  $environmentVariables: [InputEnvironmentVariable!]!
) {
  updateDeploymentVariables(
    deploymentUuid: $deploymentUuid,
    releaseName: $releaseName,
    environmentVariables: $environmentVariables
  ) {
    key
    value
    isSecret
  }
}`;


function EnvVarMigrateButton({
  route, headers, existsInRemote, sendData, isAstro, deploymentId, releaseName
}) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const toast = useToast();
  const [exists, setExists] = useState(existsInRemote);

  const errFn = (err) => {
    setExists(false);
    setLoading(false);
    toast({
      title: err.response?.data?.error || err.response?.data || err.message,
      status: 'error',
      isClosable: true,
    });
    setError(err);
  }

  function handleSoftwareClick() {
    // POST https://houston.BASEDOMAIN/v1
    setLoading(true);
    axios.post(
      route,
      {
        operationName: "deploymentVariables",
        query: getDeploymentsQuery,
        variables: {
          "deploymentUuid": deploymentId,
          "releaseName": releaseName,
        }
      },
      { headers }
    )
      .then((res) => {
        let variables = res.data?.data?.deploymentVariables || [];
        // TODO - DEDUPE? Check if key already exists and reject
        variables.push(sendData);
        axios.post(
            route,
            {
              operationName: "UpdateDeploymentVariables",
              query: updateDeploymentVariablesMutation,
              variables: {
                "deploymentUuid": deploymentId,
                "releaseName": releaseName,
                "environmentVariables": variables,
              }
            },
            { headers }
        )
          .then((res) => {
            setLoading(false);
            setExists(res.status === 200);
          })
          .catch(errFn);
      })
      .catch(errFn);
  }

  function handleAstroClick() {
    setLoading(true);
    // GET/POST https://api.astronomer.io/platform/v1beta1/organizations/:organizationId/deployments/:deploymentId
    axios.get(route, { headers })
      .then((res) => {
        // TODO - DEDUPE? Check if key already exists and reject
        res.data?.environmentVariables.push(sendData);
        axios.post(route, res.data, { headers })
        .then((res) => {
          setLoading(false);
          setExists(res.status === 200);
        })
        .catch(errFn);
      })
      .catch(errFn);
  }
  return (
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
      onClick={() => isAstro ? handleAstroClick() : handleSoftwareClick()}
    >
      {exists ? 'Ok' : loading ? '' : error ? 'Error!' : 'Migrate'}
    </Button>
  );
}

EnvVarMigrateButton.propTypes = {
  route: PropTypes.string.isRequired,
  headers: PropTypes.objectOf(PropTypes.string),
  existsInRemote: PropTypes.bool,
  // eslint-disable-next-line react/forbid-prop-types
  sendData: PropTypes.object.isRequired,
  deploymentId: PropTypes.string,
  releaseName: PropTypes.string,
};
EnvVarMigrateButton.defaultProps = {
  headers: {},
  existsInRemote: false,
  deploymentId: null,
  releaseName: null,
};

const columnHelper = createColumnHelper();
const valueColumn = columnHelper.accessor('value', {
  id: 'value', cell: (props) => <HiddenValue value={props.getValue()} />,
});

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
    localRoute(constants.ENV_VAR_ROUTE),
    remoteRoute(state.targetUrl, constants.ENV_VAR_ROUTE),
    state.token,
    () => dispatch({ type: 'set-env-loading' }),
    (res, rRes) => dispatch({
      type: 'set-env-data', envLocalData: res.data, envRemoteData: rRes.data,
    }),
    (err) => dispatch({ type: 'set-env-error', error: err }),
  );
  useEffect(() => fetchPageData(), []);
  useEffect(
    () => {
      setData(setEnvData(state.envLocalData, state.envRemoteData))
    },
    [state],
  );
  //

  // noinspection JSCheckFunctionSignatures
  const columns = [
    columnHelper.accessor('key'),
    valueColumn,
    columnHelper.display({
      id: 'migrate',
      header: 'Migrate',
      // eslint-disable-next-line react/no-unstable-nested-components
      cell: (info) => (
         <EnvVarMigrateButton
          route={
            state.isAstro ?
              proxyUrl(getAstroEnvVarRoute(state.organizationId, state.deploymentId)) :
              proxyUrl(getHoustonRoute(state.urlOrgPart))
          }
          headers={proxyHeaders(state.token)}
          existsInRemote={info.row.original.exists}
          sendData={{
            key: info.row.getValue('key'),
            value: info.row.getValue('value'),
            isSecret: false
          }}
          deploymentId={state.deploymentId}
          releaseName={state.releaseName}
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
