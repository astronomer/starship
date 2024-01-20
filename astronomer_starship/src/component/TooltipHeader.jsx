import { IconButton, Tooltip } from '@chakra-ui/react';
import { QuestionIcon } from '@chakra-ui/icons';
import React from 'react';
import PropTypes from 'prop-types';

export default function TooltipHeader({ tooltip }) {
  return (
    <Tooltip hasArrow label={tooltip}>
      <IconButton variant="ghost" aria-label="Info" icon={<QuestionIcon />} />
    </Tooltip>
  );
}
TooltipHeader.propTypes = {
  tooltip: PropTypes.string.isRequired,
};
