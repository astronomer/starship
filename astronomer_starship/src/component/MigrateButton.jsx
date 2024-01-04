import React from 'react';
import axios from 'axios';
import { Button, useToast } from '@chakra-ui/react';
import { MdErrorOutline } from 'react-icons/md';
import { FaCheck } from 'react-icons/fa';
import { GoUpload } from 'react-icons/go';
import PropTypes from 'prop-types';

export default function MigrateButton({ route, data }) {
  const [loading, setLoading] = React.useState(false);
  const [exists, setExists] = React.useState(false);
  const [error, setError] = React.useState(null);
  const toast = useToast();

  function handleClick() {
    setLoading(true);
    axios.post(route, data)
      .then((res) => {
        setExists(res.status === 200);
      })
      .catch((err) => {
        toast({
          title: err.response?.data?.error || err.message,
          status: 'error',
          isClosable: true,
        });
        setError(err);
      });
    setLoading(false);
  }
  return (
    <>
      {loading ? <Button isLoading colorScheme="teal" variant="solid" /> : null}
      {error ? <Button leftIcon={<MdErrorOutline />} colorScheme="red">Error!</Button> : null}
      {exists ? <Button leftIcon={<FaCheck />} colorScheme="green"> Ok</Button> : null}
      {!error && !loading && !exists ? (
        <Button leftIcon={<GoUpload />} onClick={() => handleClick()}>
          Migrate
        </Button>
      ) : null}
    </>
  );
}

MigrateButton.propTypes = {
  route: PropTypes.string.isRequired,
  // eslint-disable-next-line react/forbid-prop-types
  data: PropTypes.any.isRequired,
};
