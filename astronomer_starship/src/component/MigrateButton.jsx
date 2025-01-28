/* eslint-disable no-nested-ternary */
import React, { useState } from 'react';
import axios from 'axios';
import { Button, useToast } from '@chakra-ui/react';
import { MdErrorOutline, MdDeleteForever } from 'react-icons/md';
import { GoUpload } from 'react-icons/go';
import PropTypes from 'prop-types';

function checkStatus(status, exists) {
  if (status === 204)
    return false;
  return status === 200 || exists;
}

export default function MigrateButton({
  route, headers, existsInRemote, sendData, isDisabled,
}) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const toast = useToast();
  const [exists, setExists] = useState(existsInRemote);
  function handleClick() {
    setLoading(true);
    axios({
      method: exists ? 'delete' : 'post',
      url: route,
      headers,
      data: sendData,
    })
      .then((res) => {
        setLoading(false);
        setExists(checkStatus(res.status, exists));
        toast({
          title: 'Success',
          status: 'success',
          isClosable: true,
        })
      })
      .catch((err) => {
        setExists(exists);
        setLoading(false);
        toast({
          title: err.response?.data?.error || err.response?.data || err.message,
          status: 'error',
          isClosable: true,
        });
        setError(err);
      });
  }
  return (
    <Button
      isDisabled={loading || isDisabled}
      isLoading={loading}
      loadingText="Loading"
      variant="solid"
      leftIcon={(
        error ? <MdErrorOutline /> : exists ? <MdDeleteForever /> : !loading ? <GoUpload /> : <span />
        )}
      colorScheme={
        exists ? 'red' : loading ? 'teal' : error ? 'red' : 'teal'
        }
      onClick={() => handleClick()}
    >
      {exists ? 'Delete' : loading ? '' : error ? 'Error!' : 'Migrate'}
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
};
MigrateButton.defaultProps = {
  headers: {},
  existsInRemote: false,
  isDisabled: false,
};
