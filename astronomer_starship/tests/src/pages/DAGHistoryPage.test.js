// eslint-disable-next-line import/no-extraneous-dependencies
import { expect, test } from 'vitest';
import { setDagData } from '../../../src/pages/DAGHistoryPage';

test('setDagData creates {<dag_id>: {local: {...}, remote: {...}}', () => {
  const local = [{ dag_id: 'foo', other: 2 }];
  const remote = [{ dag_id: 'foo', other: 2 }];
  const expected = {
    foo: {
      local: { dag_id: 'foo', other: 2 },
      remote: { dag_id: 'foo', other: 2 },
    },
  };
  expect(setDagData(local, remote)).toStrictEqual(expected);
});
