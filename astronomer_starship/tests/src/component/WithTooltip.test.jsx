import React from 'react';
import { describe, expect, test } from 'vitest';
import { render, screen } from '@testing-library/react';
import { ChakraProvider } from '@chakra-ui/react';
import PropTypes from 'prop-types';
import WithTooltip from '../../../src/component/WithTooltip';

// Wrapper for Chakra UI components
function ChakraWrapper({ children }) {
  return <ChakraProvider>{children}</ChakraProvider>;
}

ChakraWrapper.propTypes = {
  children: PropTypes.node.isRequired,
};

describe('WithTooltip', () => {
  test('renders children directly when isDisabled is false', () => {
    render(
      <ChakraWrapper>
        <WithTooltip isDisabled={false}>
          <button type="button">Click me</button>
        </WithTooltip>
      </ChakraWrapper>,
    );

    expect(screen.getByRole('button', { name: 'Click me' })).toBeInTheDocument();
  });

  test('renders children directly when isDisabled is undefined', () => {
    render(
      <ChakraWrapper>
        <WithTooltip>
          <span>Content</span>
        </WithTooltip>
      </ChakraWrapper>,
    );

    expect(screen.getByText('Content')).toBeInTheDocument();
  });

  test('wraps children in tooltip when isDisabled is a string', () => {
    render(
      <ChakraWrapper>
        <WithTooltip isDisabled="This is disabled">
          <button type="button">Disabled button</button>
        </WithTooltip>
      </ChakraWrapper>,
    );

    // Button should still be rendered
    expect(screen.getByRole('button', { name: 'Disabled button' })).toBeInTheDocument();
  });

  test('renders correctly with complex children', () => {
    render(
      <ChakraWrapper>
        <WithTooltip isDisabled={false}>
          <div>
            <span>Nested</span>
            <span>Content</span>
          </div>
        </WithTooltip>
      </ChakraWrapper>,
    );

    expect(screen.getByText('Nested')).toBeInTheDocument();
    expect(screen.getByText('Content')).toBeInTheDocument();
  });
});
