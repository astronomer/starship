import React from 'react';
import { Tooltip } from '@chakra-ui/react';
import PropTypes from 'prop-types';

/**
 * Wrapper component that conditionally renders a tooltip around children.
 * If isDisabled is true and tooltipText is provided, shows a tooltip.
 * Otherwise, renders children directly without a tooltip.
 */
function WithTooltip({ isDisabled = false, tooltipText = '', children }) {
  return isDisabled && tooltipText ? (
    <Tooltip hasArrow label={tooltipText}>{children}</Tooltip>
  ) : children;
}

WithTooltip.propTypes = {
  isDisabled: PropTypes.bool,
  tooltipText: PropTypes.string,
  children: PropTypes.node.isRequired,
};

export default WithTooltip;
