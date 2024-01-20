import { Switch, Tooltip, WrapItem } from '@chakra-ui/react';
import React from 'react';
import axios from 'axios';
import PropTypes from 'prop-types';
import { proxyHeaders } from '../util';

function handleClick(url, token, dagId, isPaused) {
  return axios
    .patch(url, { dag_id: dagId, is_paused: isPaused }, { headers: proxyHeaders(token) })
    .then((res) => res.data)
    .catch((err) => err);
}

export default function PauseDAGButton({
  url, token, dagId, isPaused,
}) {
  return (
    <Switch
      colorScheme="teal"
      isChecked={!isPaused}
      onChange={(e) => handleClick(url, token, dagId, e.target.checked)}
    />
  );
}
PauseDAGButton.propTypes = {
  url: PropTypes.string.isRequired,
  token: PropTypes.string.isRequired,
  dagId: PropTypes.string.isRequired,
  isPaused: PropTypes.bool.isRequired,
};
