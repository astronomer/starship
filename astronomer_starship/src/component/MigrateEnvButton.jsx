/* eslint-disable no-nested-ternary */
import React, { useState } from 'react';
import axios from 'axios';
import { Button, useToast } from '@chakra-ui/react';
import { MdErrorOutline } from 'react-icons/md';
import { FaCheck } from 'react-icons/fa';
import { GoUpload } from 'react-icons/go';
import PropTypes from 'prop-types';

export default function MigrateEnvButton({
  isAstro, route, headers, existsInRemote, sendData,
}) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const toast = useToast();
  const [exists, setExists] = useState(existsInRemote);
  function handleClick() {
    setLoading(true);
    axios.post(route, sendData, { headers })
      .then((res) => {
        setLoading(false);
        setExists(res.status === 200);
      })
      .catch((err) => {
        setExists(false);
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
      isDisabled={loading || exists}
      isLoading={loading}
      loadingText="Loading"
      variant="solid"
      leftIcon={(
          error ? <MdErrorOutline /> : exists ? <FaCheck /> : !loading ? <GoUpload /> : <span />
        )}
      colorScheme={
          exists ? 'green' : loading ? 'teal' : error ? 'red' : 'teal'
        }
      onClick={() => handleClick()}
    >
      {exists ? 'Ok' : loading ? '' : error ? 'Error!' : 'Migrate'}
    </Button>
  );
}

MigrateEnvButton.propTypes = {
  route: PropTypes.string.isRequired,
  headers: PropTypes.objectOf(PropTypes.string),
  existsInRemote: PropTypes.bool,
  // eslint-disable-next-line react/forbid-prop-types
  sendData: PropTypes.object.isRequired,
};
MigrateEnvButton.defaultProps = {
  headers: {},
  existsInRemote: false,
};
