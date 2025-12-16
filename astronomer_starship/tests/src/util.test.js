/* eslint-disable import/no-extraneous-dependencies */
import { describe, expect, test } from 'vitest';
import {
  tokenUrlFromAirflowUrl,
  getTargetUrlFromParts,
  parseAirflowUrl,
  localRoute,
  proxyUrl,
  proxyHeaders,
  objectWithoutKey,
  getAstroEnvVarRoute,
  getHoustonRoute,
  getDagViewPath,
} from '../../src/util';

describe('tokenUrlFromAirflowUrl', () => {
  test('returns Astro token URL for astronomer.run URLs', () => {
    expect(tokenUrlFromAirflowUrl('https://abc123.astronomer.run/xyz789'))
      .toBe('https://cloud.astronomer.io/token');
  });

  test('returns Software token URL for deployments URLs', () => {
    expect(tokenUrlFromAirflowUrl('https://deployments.mycompany.com/release-name/airflow'))
      .toBe('https://mycompany.com/token');
  });

  test('handles URLs without deployments prefix', () => {
    expect(tokenUrlFromAirflowUrl('https://airflow.mycompany.com/home'))
      .toBe('https://airflow.mycompany.com/token');
  });
});

describe('getTargetUrlFromParts', () => {
  test('constructs Astro URL correctly', () => {
    expect(getTargetUrlFromParts('abc123', 'xyz789', true))
      .toBe('https://abc123.astronomer.run/xyz789');
  });

  test('constructs Software URL correctly', () => {
    expect(getTargetUrlFromParts('mycompany.com', 'release-name-1234', false))
      .toBe('https://deployments.mycompany.com/release-name-1234/airflow');
  });
});

describe('parseAirflowUrl', () => {
  describe('Astro URLs', () => {
    test('parses standard Astro URL', () => {
      const result = parseAirflowUrl('https://abc123.astronomer.run/xyz789');
      expect(result).toEqual({
        targetUrl: 'https://abc123.astronomer.run/xyz789',
        urlOrgPart: 'abc123',
        urlDeploymentPart: 'xyz789',
        isAstro: true,
        isValid: true,
      });
    });

    test('parses Astro URL with /home suffix', () => {
      const result = parseAirflowUrl('https://abc123.astronomer.run/xyz789/home');
      expect(result).toEqual({
        targetUrl: 'https://abc123.astronomer.run/xyz789',
        urlOrgPart: 'abc123',
        urlDeploymentPart: 'xyz789',
        isAstro: true,
        isValid: true,
      });
    });

    test('parses Astro URL without protocol', () => {
      const result = parseAirflowUrl('abc123.astronomer.run/xyz789');
      expect(result.isValid).toBe(true);
      expect(result.isAstro).toBe(true);
      expect(result.urlOrgPart).toBe('abc123');
    });
  });

  describe('Software URLs', () => {
    test('parses standard Software URL', () => {
      const result = parseAirflowUrl('https://deployments.mycompany.com/release-name-1234/airflow');
      expect(result).toEqual({
        targetUrl: 'https://deployments.mycompany.com/release-name-1234/airflow',
        urlOrgPart: 'mycompany.com',
        urlDeploymentPart: 'release-name-1234',
        isAstro: false,
        isValid: true,
      });
    });

    test('parses Software URL with /home suffix', () => {
      const result = parseAirflowUrl('https://deployments.mycompany.com/release-name-1234/airflow/home');
      expect(result.isValid).toBe(true);
      expect(result.isAstro).toBe(false);
      expect(result.urlDeploymentPart).toBe('release-name-1234');
    });
  });

  describe('Invalid URLs', () => {
    test('returns invalid for empty string', () => {
      const result = parseAirflowUrl('');
      expect(result.isValid).toBe(false);
    });

    test('returns invalid for null', () => {
      const result = parseAirflowUrl(null);
      expect(result.isValid).toBe(false);
    });

    test('returns invalid for non-string', () => {
      const result = parseAirflowUrl(123);
      expect(result.isValid).toBe(false);
    });

    test('returns invalid for malformed URL', () => {
      const result = parseAirflowUrl('not-a-valid-url');
      expect(result.isValid).toBe(false);
    });

    test('returns invalid for unknown URL format', () => {
      const result = parseAirflowUrl('https://random-domain.com/some/path');
      expect(result.isValid).toBe(false);
    });
  });
});

describe('localRoute', () => {
  test('constructs local route correctly', () => {
    const result = localRoute('/api/starship/info');
    expect(result).toBe('http://localhost:8080/api/starship/info');
  });
});

describe('proxyUrl', () => {
  test('constructs proxy URL with encoded parameter', () => {
    const result = proxyUrl('https://example.com/api?foo=bar');
    expect(result).toContain('/starship/proxy?url=');
    expect(result).toContain(encodeURIComponent('https://example.com/api?foo=bar'));
  });
});

describe('proxyHeaders', () => {
  test('returns headers with Starship-Proxy-Token', () => {
    const result = proxyHeaders('my-token-123');
    expect(result).toEqual({
      'Starship-Proxy-Token': 'my-token-123',
    });
  });
});

describe('objectWithoutKey', () => {
  test('removes specified key from object', () => {
    const obj = { a: 1, b: 2, c: 3 };
    const result = objectWithoutKey(obj, 'b');
    expect(result).toEqual({ a: 1, c: 3 });
  });

  test('returns copy of object if key does not exist', () => {
    const obj = { a: 1, b: 2 };
    const result = objectWithoutKey(obj, 'c');
    expect(result).toEqual({ a: 1, b: 2 });
  });
});

describe('getAstroEnvVarRoute', () => {
  test('constructs Astro API route correctly', () => {
    const result = getAstroEnvVarRoute('org-123', 'deploy-456');
    expect(result).toBe(
      'https://api.astronomer.io/platform/v1beta1/organizations/org-123/deployments/deploy-456',
    );
  });
});

describe('getHoustonRoute', () => {
  test('constructs Houston API route correctly', () => {
    const result = getHoustonRoute('mycompany.com');
    expect(result).toBe('https://houston.mycompany.com/v1/');
  });
});

describe('getDagViewPath', () => {
  test('returns grid view path for Airflow 2.x', () => {
    expect(getDagViewPath('my_dag', '2.8.1')).toBe('/dags/my_dag/grid');
    expect(getDagViewPath('my_dag', '2.0.0')).toBe('/dags/my_dag/grid');
  });

  test('returns simple path for Airflow 3.x', () => {
    expect(getDagViewPath('my_dag', '3.0.0')).toBe('/dags/my_dag');
    expect(getDagViewPath('my_dag', '3.1.0')).toBe('/dags/my_dag');
  });

  test('defaults to Airflow 2.x path for undefined version', () => {
    expect(getDagViewPath('my_dag', undefined)).toBe('/dags/my_dag/grid');
    expect(getDagViewPath('my_dag', null)).toBe('/dags/my_dag/grid');
  });
});
