import React, { useState, useCallback } from 'react';
import axios from 'axios';
import { Button, useToast } from '@chakra-ui/react';
import { MdErrorOutline, MdDeleteForever } from 'react-icons/md';
import { GoUpload } from 'react-icons/go';
import PropTypes from 'prop-types';

/**
 * Button for migrating or deleting items between Airflow instances
 *
 * @param {string} route - API endpoint for the migration
 * @param {Object} headers - HTTP headers for the request
 * @param {boolean} existsInRemote - Whether item already exists in remote
 * @param {Object} sendData - Data to send with the request
 * @param {boolean} isDisabled - Whether button is disabled
 * @param {Function} onStatusChange - Callback when status changes
 * @param {string} itemName - Display name for toast messages (e.g., "Variable 'my_var'")
 */
export default function MigrateButton({
  route,
  headers = {},
  existsInRemote = false,
  sendData,
  isDisabled = false,
  onStatusChange = null,
  itemName = 'item',
}) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [exists, setExists] = useState(existsInRemote);
  const toast = useToast();

  const handleClick = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const response = await axios({
        method: exists ? 'delete' : 'post',
        url: route,
        headers,
        data: sendData,
      });

      // Determine new status based on response
      const isSuccess = [200, 201, 204].includes(response.status);
      const newStatus = isSuccess ? !exists : exists;

      setExists(newStatus);
      onStatusChange?.(newStatus);

      toast({
        title: exists
          ? `Deleted ${itemName} from remote`
          : `Migrated ${itemName} to remote`,
        description: exists
          ? 'Successfully removed from target Airflow instance'
          : 'Successfully copied to target Airflow instance',
        status: 'success',
        duration: 4000,
        isClosable: true,
        variant: 'outline',
      });
    } catch (err) {
      setError(err);
      const action = exists ? 'delete' : 'migrate';
      toast({
        title: `Failed to ${action} ${itemName}`,
        description: err.response?.data?.error || err.message,
        status: 'error',
        duration: 6000,
        isClosable: true,
        variant: 'outline',
      });
    } finally {
      setLoading(false);
    }
  }, [exists, route, headers, sendData, onStatusChange, toast, itemName]);

  // Determine button appearance based on state
  const getButtonContent = () => {
    if (error) return 'Error!';
    if (exists) return 'Delete';
    return 'Migrate';
  };

  const getButtonIcon = () => {
    if (error) return <MdErrorOutline />;
    if (exists) return <MdDeleteForever />;
    return <GoUpload />;
  };

  const getColorScheme = () => {
    if (error || exists) return 'red';
    return 'green';
  };

  return (
    <Button
      size="sm"
      variant="outline"
      isDisabled={isDisabled}
      isLoading={loading}
      loadingText={exists ? 'Deleting...' : 'Migrating...'}
      onClick={handleClick}
      colorScheme={getColorScheme()}
      leftIcon={getButtonIcon()}
    >
      {getButtonContent()}
    </Button>
  );
}

MigrateButton.propTypes = {
  route: PropTypes.string.isRequired,
  headers: PropTypes.objectOf(PropTypes.string),
  existsInRemote: PropTypes.bool,
  // eslint-disable-next-line react/forbid-prop-types
  sendData: PropTypes.object.isRequired,
  isDisabled: PropTypes.bool,
  onStatusChange: PropTypes.func,
  itemName: PropTypes.string,
};
