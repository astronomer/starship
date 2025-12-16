import React, { useState } from 'react';
import { Button, useToast } from '@chakra-ui/react';
import PropTypes from 'prop-types';
import axios from 'axios';
import { MdErrorOutline } from 'react-icons/md';
import { FaCheck } from 'react-icons/fa';
import { GoUpload } from 'react-icons/go';

import { getDeploymentsQuery, updateDeploymentVariablesMutation } from '../constants';

/**
 * Custom migrate button for environment variables.
 * Handles different API patterns for Astro vs Software deployments.
 */
export default function EnvVarMigrateButton({
  route,
  headers = {},
  existsInRemote = false,
  sendData,
  isAstro,
  deploymentId = null,
  releaseName = null,
  onStatusChange = null,
}) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const toast = useToast();
  const [exists, setExists] = useState(existsInRemote);

  const handleError = (err) => {
    setExists(false);
    setLoading(false);
    toast({
      title: `Failed to migrate environment variable "${sendData?.key}"`,
      description: err.response?.data?.error || err.response?.data || err.message,
      status: 'error',
      isClosable: true,
      variant: 'outline',
      duration: 6000,
    });
    setError(err);
  };

  const handleSoftwareClick = () => {
    setLoading(true);
    axios
      .post(route, {
        operationName: 'deploymentVariables',
        query: getDeploymentsQuery,
        variables: { deploymentUuid: deploymentId, releaseName },
      }, { headers })
      .then((fetchRes) => {
        const variables = fetchRes.data?.data?.deploymentVariables || [];
        variables.push(sendData);
        return axios.post(route, {
          operationName: 'UpdateDeploymentVariables',
          query: updateDeploymentVariablesMutation,
          variables: { deploymentUuid: deploymentId, releaseName, environmentVariables: variables },
        }, { headers });
      })
      .then((updateRes) => {
        setLoading(false);
        const newStatus = updateRes.status === 200;
        setExists(newStatus);
        if (onStatusChange) onStatusChange(newStatus);
      })
      .catch(handleError);
  };

  const handleAstroClick = () => {
    setLoading(true);
    axios
      .get(route, { headers })
      .then((fetchRes) => {
        fetchRes.data?.environmentVariables.push(sendData);
        return axios.post(route, fetchRes.data, { headers });
      })
      .then((postRes) => {
        setLoading(false);
        const newStatus = postRes.status === 200;
        setExists(newStatus);
        if (onStatusChange) onStatusChange(newStatus);
      })
      .catch(handleError);
  };

  const getButtonIcon = () => {
    if (loading) return <span />;
    if (error) return <MdErrorOutline />;
    if (exists) return <FaCheck />;
    return <GoUpload />;
  };

  const getColorScheme = () => {
    if (exists) return 'green';
    if (error) return 'red';
    return 'green';
  };

  const getButtonText = () => {
    if (exists) return 'Ok';
    if (error) return 'Error!';
    return 'Migrate';
  };

  return (
    <Button
      size="sm"
      variant="outline"
      isDisabled={loading || exists}
      isLoading={loading}
      loadingText="Loading"
      leftIcon={getButtonIcon()}
      colorScheme={getColorScheme()}
      onClick={() => (isAstro ? handleAstroClick() : handleSoftwareClick())}
    >
      {getButtonText()}
    </Button>
  );
}

EnvVarMigrateButton.propTypes = {
  route: PropTypes.string.isRequired,
  headers: PropTypes.objectOf(PropTypes.string),
  existsInRemote: PropTypes.bool,
  isAstro: PropTypes.bool.isRequired,
  sendData: PropTypes.shape({
    key: PropTypes.string,
    value: PropTypes.string,
    isSecret: PropTypes.bool,
  }).isRequired,
  deploymentId: PropTypes.string,
  releaseName: PropTypes.string,
  onStatusChange: PropTypes.func,
};
