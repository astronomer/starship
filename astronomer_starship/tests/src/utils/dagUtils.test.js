import { describe, expect, test } from 'vitest';
import mergeDagData from '../../../src/utils/dagUtils';

describe('mergeDagData', () => {
  test('creates array of {local, remote} objects', () => {
    const local = [{ dag_id: 'foo', other: 2 }];
    const remote = [{ dag_id: 'foo', other: 2 }];
    const expected = [
      {
        local: { dag_id: 'foo', other: 2 },
        remote: { dag_id: 'foo', other: 2 },
      },
    ];
    expect(mergeDagData(local, remote)).toStrictEqual(expected);
  });

  test('handles missing remote data', () => {
    const local = [{ dag_id: 'bar', value: 1 }];
    const remote = [];
    const expected = [
      {
        local: { dag_id: 'bar', value: 1 },
        remote: null,
      },
    ];
    expect(mergeDagData(local, remote)).toStrictEqual(expected);
  });

  test('handles multiple DAGs with mixed matches', () => {
    const local = [
      { dag_id: 'dag_1', run_count: 10 },
      { dag_id: 'dag_2', run_count: 20 },
      { dag_id: 'dag_3', run_count: 30 },
    ];
    const remote = [
      { dag_id: 'dag_1', run_count: 5 },
      { dag_id: 'dag_3', run_count: 15 },
    ];
    const result = mergeDagData(local, remote);

    expect(result).toHaveLength(3);
    expect(result.find((d) => d.local.dag_id === 'dag_1').remote).toEqual({ dag_id: 'dag_1', run_count: 5 });
    expect(result.find((d) => d.local.dag_id === 'dag_2').remote).toBeNull();
    expect(result.find((d) => d.local.dag_id === 'dag_3').remote).toEqual({ dag_id: 'dag_3', run_count: 15 });
  });

  test('handles empty local data', () => {
    const local = [];
    const remote = [{ dag_id: 'orphan', value: 1 }];
    const result = mergeDagData(local, remote);
    expect(result).toHaveLength(0);
  });

  test('preserves all local DAG properties', () => {
    const local = [{
      dag_id: 'test_dag',
      fileloc: '/dags/test.py',
      owners: ['owner1'],
      tags: ['tag1', 'tag2'],
      schedule_interval: '@daily',
      dag_run_count: 100,
    }];
    const remote = [];
    const result = mergeDagData(local, remote);

    expect(result[0].local).toEqual(local[0]);
  });

  test('preserves all remote DAG properties', () => {
    const local = [{ dag_id: 'test_dag' }];
    const remote = [{
      dag_id: 'test_dag',
      fileloc: '/dags/remote.py',
      owners: ['remote_owner'],
      is_paused: true,
      dag_run_count: 50,
    }];
    const result = mergeDagData(local, remote);

    expect(result[0].remote).toEqual(remote[0]);
  });

  test('ignores remote DAGs not in local', () => {
    const local = [{ dag_id: 'local_only' }];
    const remote = [
      { dag_id: 'local_only', value: 1 },
      { dag_id: 'remote_only', value: 2 },
    ];
    const result = mergeDagData(local, remote);

    expect(result).toHaveLength(1);
    expect(result[0].local.dag_id).toBe('local_only');
  });
});
