import React, { useState, useCallback } from 'react';
import axios from 'axios';
import { Button, useToast } from '@chakra-ui/react';
import { MdErrorOutline, MdDeleteForever } from 'react-icons/md';
import { GoUpload } from 'react-icons/go';
import PropTypes from 'prop-types';

/**
 * Button for migrating or deleting items between Airflow instances
 */
export default function MigrateButton({
  route,
  headers = {},
  existsInRemote = false,
  sendData,
  isDisabled = false,
  onStatusChange,
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
        title: exists ? 'Deleted successfully' : 'Migrated successfully',
        status: 'success',
        duration: 3000,
        isClosable: true,
      });
    } catch (err) {
      setError(err);
      toast({
        title: 'Operation failed',
        description: err.response?.data?.error || err.message,
        status: 'error',
        duration: 5000,
        isClosable: true,
      });
    } finally {
      setLoading(false);
    }
  }, [exists, route, headers, sendData, onStatusChange, toast]);

  // Determine button appearance based on state
  const getButtonProps = () => {
    if (error) {
      return {
        colorScheme: 'red',
        leftIcon: <MdErrorOutline />,
        children: 'Error!',
      };
    }
    if (exists) {
      return {
        colorScheme: 'red',
        leftIcon: <MdDeleteForever />,
        children: 'Delete',
      };
    }
    return {
      colorScheme: 'green',
      leftIcon: <GoUpload />,
      children: 'Migrate',
    };
  };

  const buttonProps = getButtonProps();

  return (
      <Button
        size="sm"
        variant="outline"
        isDisabled={isDisabled}
      isLoading={loading}
      loadingText={exists ? 'Deleting...' : 'Migrating...'}
      onClick={handleClick}
      {...buttonProps}
    />
  );
}

MigrateButton.propTypes = {
  route: PropTypes.string.isRequired,
  headers: PropTypes.objectOf(PropTypes.string),
  existsInRemote: PropTypes.bool,
  sendData: PropTypes.object.isRequired,
  isDisabled: PropTypes.bool,
  onStatusChange: PropTypes.func,
};
