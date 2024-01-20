import React, { useEffect, useState } from 'react';
import { createColumnHelper } from '@tanstack/react-table';
import {
  Button, IconButton,
  Tag, Text, Tooltip,
} from '@chakra-ui/react';
import PropTypes from 'prop-types';
import { QuestionIcon } from '@chakra-ui/icons';
import MigrateButton from '../component/MigrateButton';
import StarshipPage from '../component/StarshipPage';
import { fetchData, proxyHeaders, proxyUrl } from '../util';
import constants from '../constants';
import PauseDAGButton from '../component/PauseDAGButton';
import TooltipHeader from '../component/TooltipHeader.jsx';

const description = (
  <Text fontSize="xl">
    DAGs and Task History can be migrated to prevent Airflow from re-doing old runs.
    DAGs can be paused or unpaused on either Airflow instance.
  </Text>
);
const columnHelper = createColumnHelper();
const scheduleColumn = columnHelper.accessor('schedule_interval', {
  id: 'schedule_interval',
  cell: (props) => (
    <Tag
      // eslint-disable-next-line react/no-children-prop
      children={props.getValue()}
    />
  ),
});
const tagsColumn = columnHelper.accessor('tags', {
  id: 'tags',
  cell: (props) => (
    <Tag
      colorScheme="teal"
      // eslint-disable-next-line react/no-children-prop
      children={props.getValue()}
    />
  ),
});

function setDagData(localData, remoteData) {
  return localData;
  // return localData.map(
  //   (d) => ({
  //     ...d,
  //     exists: remoteData.map(
  //       // eslint-disable-next-line camelcase
  //       ({ dag_id }) => dag_id,
  //     ).includes(d.dag_id),
  //   }),
  // );
}

export default function DAGHistoryPage({ state, dispatch }) {
  const [data, setData] = useState(setDagData(state.dagsLocalData, []));
  useEffect(() => {
    fetchData(
      constants.DAGS_ROUTE,
      state.targetUrl + constants.DAGS_ROUTE,
      state.token,
      () => dispatch({ type: 'set-dags-loading' }),
      (res, rRes) => dispatch({
        type: 'set-dags-data', dagsLocalData: res.data, dagsRemoteData: rRes.data,
      }),
      (err) => dispatch({ type: 'set-dags-error', error: err }),
      dispatch,
    );
  }, []);
  useEffect(
    () => setData(setDagData(state.dagsLocalData, state.dagsRemoteData)),
    [state.dagsLocalData, state.dagsRemoteData],
  );

  // noinspection JSCheckFunctionSignatures
  const columns = [
    columnHelper.accessor('dag_id'),
    scheduleColumn,
    columnHelper.accessor('fileloc'),
    columnHelper.accessor('description'),
    columnHelper.accessor('owners'),
    tagsColumn,
    columnHelper.display({
      id: 'local',
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
      // eslint-disable-next-line react/no-unstable-nested-components
      cell: (info) => (
        <PauseDAGButton
          dagId={info.row.getValue('dag_id')}
          isPaused={info.row.original.is_paused}
          token={state.token}
          url={constants.DAGS_ROUTE}
        />
      ),
    }),
    columnHelper.display({
      id: 'migrate',
      header: 'Migrate History',
      // eslint-disable-next-line react/no-unstable-nested-components
      cell: (info) => (
        <MigrateButton
          route={proxyUrl(state.targetUrl + constants.DAGS_ROUTE)}
          headers={proxyHeaders(state.token)}
          existsInRemote={info.row.original.exists}
          sendData={{
            dag_id: info.row.getValue('dag_id'),
            schedule_interval: info.row.getValue('schedule_interval'),
            is_paused: info.row.original.is_paused,
            fileloc: info.row.getValue('fileloc'),
            description: info.row.getValue('description'),
            owners: info.row.getValue('owners'),
            tags: info.row.getValue('tags'),
          }}
        />
      ),
    }),
    columnHelper.display({
      id: 'remote',
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
      // eslint-disable-next-line react/no-unstable-nested-components
      cell: (info) => (
        <PauseDAGButton
          dagId={info.row.getValue('dag_id')}
          isPaused={info.row.original.is_paused}
          token={state.token}
          url={proxyUrl(state.targetUrl + constants.DAGS_ROUTE)}
        />
      ),
    }),
  ];

  return (
    <StarshipPage
      description={description}
      loading={state.dagsLoading}
      data={data}
      columns={columns}
      error={state.dagsError}
    />
  );
}
DAGHistoryPage.propTypes = {
  // eslint-disable-next-line react/forbid-prop-types
  state: PropTypes.object.isRequired,
  dispatch: PropTypes.func.isRequired,
};
