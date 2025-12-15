// eslint-disable-next-line import/no-extraneous-dependencies
import { expect, test } from 'vitest';
import mergeDagData from '../../../src/utils/dagUtils';

test('mergeDagData creates array of {local: {...}, remote: {...}} objects', () => {
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

test('mergeDagData handles missing remote data', () => {
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
