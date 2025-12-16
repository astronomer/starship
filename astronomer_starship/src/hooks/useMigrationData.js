import { useState, useCallback, useEffect } from 'react';
import axios from 'axios';
import { useToast } from '@chakra-ui/react';

import { useAppDispatch, useTargetConfig } from '../AppContext';
import {
  localRoute, proxyUrl, proxyHeaders, objectWithoutKey,
} from '../util';

/**
 * Custom hook for managing migration data between local and remote Airflow instances.
 * Provides shared logic for fetching, merging, and migrating items.
 *
 * @param {Object} options - Configuration options
 * @param {string} options.route - API route constant
 * @param {string} options.idField - Field name to identify items
 * @param {string} options.itemName - Singular name for toast messages
 * @returns {Object} - Hook state and handlers
 */
export default function useMigrationData({ route, idField, itemName }) {
  const { targetUrl, token } = useTargetConfig();
  const dispatch = useAppDispatch();
  const toast = useToast();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState([]);
  const [isMigratingAll, setIsMigratingAll] = useState(false);

  /**
   * Merge local and remote data arrays, marking items that exist in remote.
   */
  const mergeData = useCallback((localData, remoteData) => localData.map((item) => ({
    ...item,
    exists: remoteData.some((remote) => remote[idField] === item[idField]),
  })), [idField]);

  /**
   * Fetch data from both local and remote instances.
   */
  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const [localRes, remoteRes] = await Promise.all([
        axios.get(localRoute(route)),
        axios.get(proxyUrl(targetUrl + route), {
          headers: proxyHeaders(token),
        }),
      ]);

      if (localRes.status === 200 && remoteRes.status === 200) {
        setData(mergeData(localRes.data, remoteRes.data));
      } else {
        throw new Error('Invalid response from server');
      }
    } catch (err) {
      setError(err);
      if (err.response?.status === 401) {
        dispatch({ type: 'invalidate-token' });
      }
    } finally {
      setLoading(false);
    }
  }, [targetUrl, token, dispatch, route, mergeData]);

  // Fetch data on mount and when dependencies change
  useEffect(() => {
    fetchData();
  }, [fetchData]);

  /**
   * Handle individual item status change (after migrate/delete).
   */
  const handleItemStatusChange = useCallback((id, newStatus) => {
    setData((prev) => prev.map((item) => (
      item[idField] === id ? { ...item, exists: newStatus } : item
    )));
  }, [idField]);

  /**
   * Migrate all unmigrated items to the remote instance.
   */
  const handleMigrateAll = useCallback(async () => {
    const unmigratedItems = data.filter((item) => !item.exists);
    if (unmigratedItems.length === 0) return;

    setIsMigratingAll(true);
    let successCount = 0;
    let errorCount = 0;

    // Use Promise.allSettled for parallel execution with error handling
    const results = await Promise.allSettled(
      unmigratedItems.map(async (item) => {
        const response = await axios.post(
          proxyUrl(targetUrl + route),
          objectWithoutKey(item, 'exists'),
          { headers: proxyHeaders(token) },
        );
        return { item, response };
      }),
    );

    results.forEach((result) => {
      if (result.status === 'fulfilled') {
        successCount += 1;
        const { item } = result.value;
        setData((prev) => prev.map((d) => (
          d[idField] === item[idField] ? { ...d, exists: true } : d
        )));
      } else {
        errorCount += 1;
      }
    });

    setIsMigratingAll(false);

    const pluralName = `${itemName}${successCount !== 1 ? 's' : ''}`;
    let status = 'success';
    if (successCount === 0) status = 'error';
    else if (errorCount > 0) status = 'warning';

    const getDescription = () => {
      if (successCount === 0) {
        return `All ${unmigratedItems.length} items failed to migrate. Check your connection and permissions.`;
      }
      if (errorCount > 0) {
        return `${errorCount} item${errorCount !== 1 ? 's' : ''} failed to migrate. You can retry individual items.`;
      }
      return `Successfully copied ${successCount} ${pluralName} to target Airflow instance`;
    };

    toast({
      title: successCount > 0
        ? `Migrated ${successCount} ${pluralName} to remote`
        : 'Migration failed',
      description: getDescription(),
      status,
      duration: 5000,
      isClosable: true,
      variant: 'outline',
    });
  }, [data, targetUrl, token, toast, route, idField, itemName]);

  // Calculate progress
  const totalItems = data.length;
  const migratedItems = data.filter((item) => item.exists).length;

  return {
    // State
    loading,
    error,
    data,
    isMigratingAll,
    totalItems,
    migratedItems,
    // Actions
    fetchData,
    handleItemStatusChange,
    handleMigrateAll,
    // Config (pass-through for convenience)
    targetUrl,
    token,
  };
}
