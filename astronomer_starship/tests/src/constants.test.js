import { describe, expect, test } from 'vitest';
import constants, { ROUTES } from '../../src/constants';

describe('API Route Constants', () => {
  test('all API routes start with /api/starship/', () => {
    const apiRoutes = [
      constants.TELESCOPE_ROUTE,
      constants.ENV_VAR_ROUTE,
      constants.POOL_ROUTE,
      constants.CONNECTIONS_ROUTE,
      constants.VARIABLES_ROUTE,
      constants.DAGS_ROUTE,
      constants.DAG_RUNS_ROUTE,
      constants.TASK_INSTANCE_ROUTE,
      constants.TASK_INSTANCE_HISTORY_ROUTE,
      constants.SOURCE_CONNECTION_ROUTE,
      constants.CUTOVER_WAVES_ROUTE,
      constants.CUTOVER_PURGE_ALL_ROUTE,
    ];

    apiRoutes.forEach((route) => {
      expect(route).toMatch(/^\/api\/starship\//);
    });
  });

  test('constants object is frozen', () => {
    expect(Object.isFrozen(constants)).toBe(true);
  });

  test('TELESCOPE_ROUTE is defined', () => {
    expect(constants.TELESCOPE_ROUTE).toBe('/api/starship/telescope');
  });

  test('ENV_VAR_ROUTE is defined', () => {
    expect(constants.ENV_VAR_ROUTE).toBe('/api/starship/env_vars');
  });

  test('POOL_ROUTE is defined', () => {
    expect(constants.POOL_ROUTE).toBe('/api/starship/pools');
  });

  test('CONNECTIONS_ROUTE is defined', () => {
    expect(constants.CONNECTIONS_ROUTE).toBe('/api/starship/connections');
  });

  test('VARIABLES_ROUTE is defined', () => {
    expect(constants.VARIABLES_ROUTE).toBe('/api/starship/variables');
  });

  test('DAGS_ROUTE is defined', () => {
    expect(constants.DAGS_ROUTE).toBe('/api/starship/dags');
  });

  test('DAG_RUNS_ROUTE is defined', () => {
    expect(constants.DAG_RUNS_ROUTE).toBe('/api/starship/dag_runs');
  });

  test('TASK_INSTANCE_ROUTE is defined', () => {
    expect(constants.TASK_INSTANCE_ROUTE).toBe('/api/starship/task_instances');
  });

  test('TASK_INSTANCE_HISTORY_ROUTE is defined', () => {
    expect(constants.TASK_INSTANCE_HISTORY_ROUTE).toBe('/api/starship/task_instance_history');
  });

  test('SOURCE_CONNECTION_ROUTE is defined', () => {
    expect(constants.SOURCE_CONNECTION_ROUTE).toBe('/api/starship/source_connection');
  });

  test('CUTOVER_WAVES_ROUTE is defined', () => {
    expect(constants.CUTOVER_WAVES_ROUTE).toBe('/api/starship/cutover/waves');
  });

  test('CUTOVER_PURGE_ALL_ROUTE is defined', () => {
    expect(constants.CUTOVER_PURGE_ALL_ROUTE).toBe('/api/starship/cutover/purge_all');
  });
});

describe('Navigation Route Constants', () => {
  test('ROUTES object is frozen', () => {
    expect(Object.isFrozen(ROUTES)).toBe(true);
  });

  test('all navigation routes are kebab-case path segments (no leading slash)', () => {
    // Lowercase letters, optionally hyphen-separated. Hyphens allow multi-word
    // routes like `source-setup` and `cutover-history` without using
    // underscores (unusual in URLs) or camelCase (inconsistent with existing
    // single-word routes like `setup`, `variables`, `pools`).
    Object.values(ROUTES).forEach((route) => {
      expect(route).toMatch(/^[a-z]+(-[a-z]+)*$/);
      expect(route).not.toMatch(/^\//);
    });
  });

  test('SETUP route is defined', () => {
    expect(ROUTES.SETUP).toBe('setup');
  });

  test('VARIABLES route is defined', () => {
    expect(ROUTES.VARIABLES).toBe('variables');
  });

  test('CONNECTIONS route is defined', () => {
    expect(ROUTES.CONNECTIONS).toBe('connections');
  });

  test('POOLS route is defined', () => {
    expect(ROUTES.POOLS).toBe('pools');
  });

  test('ENV_VARS route is defined', () => {
    expect(ROUTES.ENV_VARS).toBe('env');
  });

  test('DAGS route is defined', () => {
    expect(ROUTES.DAGS).toBe('dags');
  });

  test('TELESCOPE route is defined', () => {
    expect(ROUTES.TELESCOPE).toBe('telescope');
  });

  test('SOURCE_SETUP route is defined', () => {
    expect(ROUTES.SOURCE_SETUP).toBe('source-setup');
  });

  test('CUTOVER route is defined', () => {
    expect(ROUTES.CUTOVER).toBe('cutover');
  });

  test('CUTOVER_HISTORY route is defined', () => {
    expect(ROUTES.CUTOVER_HISTORY).toBe('cutover-history');
  });
});
