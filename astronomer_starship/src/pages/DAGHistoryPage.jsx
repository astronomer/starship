/* eslint-disable no-nested-ternary,react/no-unstable-nested-components */
// noinspection JSUnusedLocalSymbols
import React, { useEffect, useState } from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import {
  Badge,
  Button,
  CircularProgress,
  FormControl,
  HStack,
  InputGroup, InputLeftAddon, Link,
  NumberDecrementStepper, NumberIncrementStepper, NumberInput,
  NumberInputField, NumberInputStepper, Spacer,
  Switch,
  Tag,
  Text,
  Tooltip,
  useToast, VStack,
} from '@chakra-ui/react';
import PropTypes from 'prop-types';
import axios from 'axios';
import { MdErrorOutline, MdDeleteForever } from 'react-icons/md';
import { GrDocumentMissing } from 'react-icons/gr';
import { GoUpload } from 'react-icons/go';
import humanFormat from 'human-format';
import { ExternalLinkIcon, RepeatIcon } from '@chakra-ui/icons';
import StarshipPage from '../component/StarshipPage';
import {
  fetchData, localRoute, proxyHeaders, proxyUrl, remoteRoute,
} from '../util';
import constants from '../constants';
import TooltipHeader from '../component/TooltipHeader';

// eslint-disable-next-line react/prop-types
function WithTooltip({ isDisabled, children }) {
  return isDisabled
    ? <Tooltip hasArrow label={isDisabled}>{children}</Tooltip>
    : children;
}

function DAGHistoryMigrateButton({
  url,
  token,
  dagId,
  limit,
  existsInRemote,
  isDisabled,
  dispatch,
}) {
  // noinspection DuplicatedCode
  const [loadPerc, setLoadPerc] = useState(0);
  const [error, setError] = useState(null);
  const toast = useToast();
  const [exists, setExists] = useState(existsInRemote);
  const percent = 100;

  function handleClick() {

    function deleteRuns() {
      setLoadPerc(percent * 0.5);
      axios({
        method: 'delete',
        url: proxyUrl(url + constants.DAG_RUNS_ROUTE),
        headers: proxyHeaders(token),
        data: { dag_id: dagId },
      }).then((res) => {
        setExists(!(res.status === 204));
        dispatch({
          type: 'set-dags-data',
          dagsData: {
            [dagId]: {
              remote: {
                dag_run_count: 0,
              },
            },
          },
        });
        setLoadPerc(percent * 1);
        setLoadPerc(0);
      }).catch((err) => {
        setExists(false);
        setLoadPerc(percent * 0);
        toast({
          title: err.response?.data?.error || err.response?.data || err.message,
          status: 'error',
          isClosable: true,
        });
        setError(err);
      });
    }

    if (exists) {
      deleteRuns();
      return;
    }
    const errFn = (err) => {
      setExists(false);
      // noinspection PointlessArithmeticExpressionJS
      setLoadPerc(percent * 0);
      toast({
        title: err.response?.data?.error || err.response?.data || err.message,
        status: 'error',
        isClosable: true,
      });
      setError(err);
    };
    setLoadPerc(percent * 0.05);
    Promise.all([
      // Get both DAG Runs and Task Instances locally
      axios.get(localRoute(constants.DAG_RUNS_ROUTE), { params: { dag_id: dagId, limit } }),
      axios.get(localRoute(constants.TASK_INSTANCE_ROUTE), { params: { dag_id: dagId, limit } }),
    ]).then(
      axios.spread((dagRunsRes, taskInstanceRes) => {
        setLoadPerc(percent * 0.5);
        // Then create DAG Runs
        axios.post(
          proxyUrl(url + constants.DAG_RUNS_ROUTE),
          { dag_runs: dagRunsRes.data.dag_runs },
          { params: { dag_id: dagId }, headers: proxyHeaders(token) },
        ).then((dagRunCreateRes) => {
          if (dagRunCreateRes.status !== 200) {
            errFn({ err: { response: dagRunCreateRes } });
            return;
          }
          dispatch({
            type: 'set-dags-data',
            dagsData: {
              [dagId]: {
                remote: {
                  dag_run_count: dagRunCreateRes.data.dag_run_count,
                },
              },
            },
          });
          setLoadPerc(percent * 0.75);
          // Then create Task Instances
          axios.post(
            proxyUrl(url + constants.TASK_INSTANCE_ROUTE),
            { task_instances: taskInstanceRes.data.task_instances },
            { params: { dag_id: dagId }, headers: proxyHeaders(token) },
          ).then(
            (taskInstanceCreateRes) => {
              // noinspection PointlessArithmeticExpressionJS
              setLoadPerc(percent * 1);
              setLoadPerc(0);
              setExists(taskInstanceCreateRes.status === 200);
            },
          ).catch(errFn);
        }).catch(errFn);
      }),
    ).catch(errFn);
  }

  return (
    <WithTooltip isDisabled={isDisabled}>
      <Button
        isDisabled={isDisabled || loadPerc}
        // isLoading={loading}
        // loadingText="Loading"
        variant="solid"
        leftIcon={(
          error ? <MdErrorOutline />
            : exists ? <MdDeleteForever />
              : isDisabled ? <GrDocumentMissing />
                : !loadPerc ? <GoUpload />
                  : <span />
        )}
        colorScheme={
          exists ? 'red' : isDisabled ? 'gray' : error ? 'red' : 'teal'
        }
        onClick={() => handleClick()}
      >
        {exists ? 'Delete'
          : loadPerc ? (
            <CircularProgress thickness="20px" size="30px" value={loadPerc} />
          )
            : isDisabled ? ''
              : error ? 'Error!'
                : 'Migrate'}
      </Button>
    </WithTooltip>
  );
}

DAGHistoryMigrateButton.propTypes = {
  url: PropTypes.string.isRequired,
  token: PropTypes.string.isRequired,
  dagId: PropTypes.string.isRequired,
  limit: PropTypes.number,
  existsInRemote: PropTypes.bool,
  isDisabled: PropTypes.oneOfType([PropTypes.bool, PropTypes.string]),
  dispatch: PropTypes.func.isRequired,
};
DAGHistoryMigrateButton.defaultProps = {
  limit: 10,
  existsInRemote: false,
  isDisabled: false,
};

export function setDagData(localData, remoteData, key = 'dag_id') {
  const output = {};
  localData.forEach((i) => {
    const keyValue = i[key];
    if (!(keyValue in output)) output[keyValue] = {};
    output[keyValue].local = i;
  });
  remoteData.forEach((i) => {
    const keyValue = i[key];
    if (!(keyValue in output)) {
      // eslint-disable-next-line no-console
      console.log(`Found dag_id=${keyValue} in Remote missing in Local!`);
    } else {
      output[keyValue].remote = i;
    }
  });
  return output;
}

export default function DAGHistoryPage({ state, dispatch }) {
  const columnHelper = createColumnHelper();
  const [data, setData] = useState(Object.values(state.dagsData));
  const fetchPageData = () => fetchData(
    localRoute(constants.DAGS_ROUTE),
    remoteRoute(state.targetUrl, constants.DAGS_ROUTE),
    state.token,
    () => dispatch({ type: 'set-dags-loading' }),
    (res, rRes) => dispatch({ type: 'set-dags-data', dagsData: setDagData(res.data, rRes.data) }),
    (err) => dispatch({ type: 'set-dags-error', error: err }),
    dispatch,
  );

  function handlePausedClick(url, token, dagId, isPaused, isLocal) {
    dispatch({
      type: 'set-dags-data',
      dagsData: { [dagId]: { [isLocal ? 'local' : 'remote']: { is_paused_loading: true } } },
    });
    axios
      .patch(url, { dag_id: dagId, is_paused: isPaused }, { headers: proxyHeaders(token) })
      .then((res) => {
      // update global state
        dispatch({
          type: 'set-dags-data',
          dagsData: {
            [res.data.dag_id]: {
              [isLocal ? 'local' : 'remote']: {
                is_paused: res.data.is_paused,
                is_paused_loading: false,
              },
            },
          },
        });
      })
      .catch((err) => err);
  }

  useEffect(() => fetchPageData(), []);
  useEffect(() => setData(Object.values(state.dagsData)), [state]);

  // noinspection JSUnusedLocalSymbols,JSCheckFunctionSignatures,JSUnresolvedReference
  const columns = [
    columnHelper.accessor(
      (row) => row.local.dag_id,
      {
        id: 'dagId',
        header: 'ID',
        cell: (info) => (
          <VStack>
            <Tooltip hasArrow label={`File: ${info.row.original.local.fileloc}`}>
              <Link
                isExternal
                href={localRoute(`/dags/${info.getValue()}`)}
              >
                {info.getValue()}
                <ExternalLinkIcon mx="2px" />
              </Link>
            </Tooltip>
            <HStack>
              {info.row.original.local.tags.map(
                (tag) => <Tag colorScheme="teal" borderRadius="full" key={tag}>{tag}</Tag>,
              )}
            </HStack>
          </VStack>
        ),
      },
    ),
    columnHelper.accessor(
      (row) => row.local.schedule_interval,
      {
        id: 'schedule',
        header: 'Schedule',
        cell: (info) => <Tag>{info.getValue()}</Tag>,
      },
    ),
    columnHelper.accessor(
      (row) => row.local.description,
      {
        id: 'description',
        header: 'Description',
        cell: (info) => info.getValue(),
      },
    ),
    columnHelper.accessor(
      (row) => row.local.owners,
      {
        id: 'owners',
        header: 'Owners',
        cell: (info) => info.getValue(),
      },
    ),
    columnHelper.display({
      id: 'local_is_paused',
      header: (
        <>
          Local
          {' '}
          <TooltipHeader tooltip={
            'Use this toggle to pause/unpause a DAG. '
            + 'The scheduler will not schedule new tasks instances for a paused DAG. '
            + 'Tasks already running at pause time will not be affected'
          }
          />
        </>
      ),
      cell: (info) => (
        <>
          <Tooltip hasArrow label="DAG Run Count">
            <Badge
              marginX={1}
              fontSize="sm"
              variant="outline"
              colorScheme={info.row.original.local.dag_run_count > 0 ? 'teal' : 'red'}
            >
              {humanFormat(info.row.original.local.dag_run_count)}
            </Badge>
          </Tooltip>
          <Switch
            colorScheme="teal"
            isChecked={!info.row.original.local.is_paused}
            disabled={info.row.original.local.is_paused_loading || false}
            // eslint-disable-next-line no-unused-vars
            onChange={(e) => handlePausedClick(
              localRoute(constants.DAGS_ROUTE),
              state.token,
              info.row.original.local.dag_id,
              !info.row.original.local.is_paused,
              true,
            )}
          />
        </>
      ),
    }),
    columnHelper.display({
      id: 'migrate',
      header: 'Migrate',
      // eslint-disable-next-line react/no-unstable-nested-components
      cell: (info) => (
        <DAGHistoryMigrateButton
          url={state.targetUrl}
          token={state.token}
          dagId={info.row.original.local.dag_id}
          limit={Number(state.limit)}
          existsInRemote={!!info.row.original.remote?.dag_run_count || false}
          isDisabled={
            !info.row.original.remote?.dag_id ? 'DAG not found in remote'
              : !info.row.original.local.dag_run_count ? 'No DAG Runs to migrate'
                : false
          }
          dispatch={dispatch}
        />
      ),

    }),
    columnHelper.display({
      id: 'remote_is_paused',
      header: (
        <>
          Remote
          {' '}
          <TooltipHeader tooltip={
            'Use this toggle to pause/unpause a DAG. '
            + 'The scheduler will not schedule new tasks instances for a paused DAG. '
            + 'Tasks already running at pause time will not be affected'
          }
          />
        </>
      ),
      cell: (info) => ((info.row.original.remote || false) ? (
        <>
          <Switch
            colorScheme="teal"
            isChecked={!info.row.original.remote?.is_paused}
            disabled={info.row.original.remote?.is_paused_loading || false}
            // eslint-disable-next-line no-unused-vars
            onChange={(e) => handlePausedClick(
              proxyUrl(state.targetUrl + constants.DAGS_ROUTE),
              state.token,
              info.row.original.local.dag_id,
              !info.row.original.remote?.is_paused,
              false,
            )}
          />
          <Tooltip hasArrow label="DAG Run Count">
            <Badge
              marginX={1}
              fontSize="sm"
              variant="outline"
              colorScheme={info.row.original.remote.dag_run_count > 0 ? 'teal' : 'red'}
            >
              {humanFormat(info.row.original.remote.dag_run_count)}
            </Badge>
          </Tooltip>
          <Link
            isExternal
            href={remoteRoute(state.targetUrl, `/dags/${info.row.original.remote.dag_id}`)}
            mx="2px"
          >
            (
            <ExternalLinkIcon mx="2px" />
            )
          </Link>
        </>
      ) : null),
    }),
  ];
  return (
    <StarshipPage
      description={(
        <HStack>
          <Text fontSize="xl">
            DAGs and Task History can be migrated to prevent
            Airflow from rescheduling existing runs.
            DAGs can be paused or un-paused on either Airflow instance.
          </Text>
          <Spacer />
          <Tooltip hasArrow label="The number of DAG Runs (and associated Task Instances) to migrate">
            <FormControl width="20%" minWidth="200px">
              <InputGroup size="sm">
                <InputLeftAddon># DAG Runs</InputLeftAddon>
                <NumberInput
                  value={state.limit}
                  onChange={(e) => dispatch({ type: 'set-limit', limit: Number(e) })}
                >
                  <NumberInputField />
                  <NumberInputStepper>
                    <NumberIncrementStepper />
                    <NumberDecrementStepper />
                  </NumberInputStepper>
                </NumberInput>
              </InputGroup>
            </FormControl>
          </Tooltip>
          <Button minWidth="50px" leftIcon={<RepeatIcon />} onClick={() => fetchPageData()}>Reset</Button>
        </HStack>
      )}
      loading={state.dagsLoading}
      data={data}
      columns={columns}
      error={state.dagsError}
      resetFn={fetchPageData}
    />
  );
}
DAGHistoryPage.propTypes = {
  // eslint-disable-next-line react/forbid-prop-types
  state: PropTypes.object.isRequired,
  dispatch: PropTypes.func.isRequired,
};
