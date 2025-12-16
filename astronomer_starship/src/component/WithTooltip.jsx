import React from 'react';
import { Tooltip } from '@chakra-ui/react';
import PropTypes from 'prop-types';

/**
 * Wrapper component that conditionally renders a tooltip around children.
 * If isDisabled is a truthy string, shows a tooltip with that string as label.
 * Otherwise, renders children directly without a tooltip.
 */
function WithTooltip({ isDisabled = false, children }) {
  return isDisabled ? (
    <Tooltip hasArrow label={isDisabled}>{children}</Tooltip>
  ) : children;
}

WithTooltip.propTypes = {
  isDisabled: PropTypes.oneOfType([PropTypes.bool, PropTypes.string]),
  children: PropTypes.node.isRequired,
};

export default WithTooltip;
