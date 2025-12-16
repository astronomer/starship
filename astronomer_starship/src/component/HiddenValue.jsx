import React from 'react';
import {
  Button, Input, InputGroup, InputRightElement,
} from '@chakra-ui/react';
import PropTypes from 'prop-types';

export default function HiddenValue({ value = '' }) {
  const [show, setShow] = React.useState(false);
  const handleClick = () => setShow(!show);

  if (!value) {
    return null;
  }
  return (
    <InputGroup size="md">
      <Input pr="4.5rem" type={show ? 'text' : 'password'} value={value || ''} isReadOnly border="0" />
      <InputRightElement width="4.5rem">
        <Button h="1.75rem" size="sm" onClick={handleClick}>
          {show ? 'Hide' : 'Show'}
        </Button>
      </InputRightElement>
    </InputGroup>
  );
}
HiddenValue.propTypes = {
  value: PropTypes.string,
};
