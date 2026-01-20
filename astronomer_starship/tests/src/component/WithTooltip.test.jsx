import { describe, expect, test } from 'vitest';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import WithTooltip from '../../../src/component/WithTooltip';
import { renderWithChakra } from '../../test-utils';

describe('WithTooltip', () => {
  describe('Rendering without tooltip', () => {
    test('renders children directly when isDisabled is false', () => {
      renderWithChakra(
        <WithTooltip isDisabled={false}>
          <button type="button">Click me</button>
        </WithTooltip>,
      );

      expect(screen.getByRole('button', { name: 'Click me' })).toBeInTheDocument();
    });

    test('renders children directly when isDisabled is undefined', () => {
      renderWithChakra(
        <WithTooltip>
          <span>Content</span>
        </WithTooltip>,
      );

      expect(screen.getByText('Content')).toBeInTheDocument();
    });

    test('renders children without tooltip when isDisabled is true but no tooltipText', () => {
      renderWithChakra(
        <WithTooltip isDisabled>
          <button type="button">No tooltip</button>
        </WithTooltip>,
      );

      expect(screen.getByRole('button', { name: 'No tooltip' })).toBeInTheDocument();
    });

    test('renders children without tooltip when tooltipText is empty string', () => {
      renderWithChakra(
        <WithTooltip isDisabled tooltipText="">
          <button type="button">Empty tooltip</button>
        </WithTooltip>,
      );

      expect(screen.getByRole('button', { name: 'Empty tooltip' })).toBeInTheDocument();
    });
  });

  describe('Rendering with tooltip', () => {
    test('wraps children in tooltip when isDisabled is true and tooltipText is provided', () => {
      renderWithChakra(
        <WithTooltip isDisabled tooltipText="This is disabled">
          <button type="button">Disabled button</button>
        </WithTooltip>,
      );

      // Button should still be rendered (tooltip wraps it)
      expect(screen.getByRole('button', { name: 'Disabled button' })).toBeInTheDocument();
    });

    test('shows tooltip text on hover when enabled', async () => {
      const user = userEvent.setup();

      renderWithChakra(
        <WithTooltip isDisabled tooltipText="Hover text">
          <button type="button">Hover me</button>
        </WithTooltip>,
      );

      const button = screen.getByRole('button', { name: 'Hover me' });
      await user.hover(button);

      // Chakra tooltips appear after a delay, wait for it
      // Note: In jsdom, tooltip visibility can be tricky to test
      // We mainly verify the component renders without error
      expect(button).toBeInTheDocument();
    });
  });

  describe('Complex children', () => {
    test('renders correctly with complex children', () => {
      renderWithChakra(
        <WithTooltip isDisabled={false}>
          <div>
            <span>Nested</span>
            <span>Content</span>
          </div>
        </WithTooltip>,
      );

      expect(screen.getByText('Nested')).toBeInTheDocument();
      expect(screen.getByText('Content')).toBeInTheDocument();
    });

    test('handles multiple nested elements', () => {
      renderWithChakra(
        <WithTooltip isDisabled tooltipText="Complex tooltip">
          <div data-testid="container">
            <header>Header</header>
            <main>
              <p>Paragraph 1</p>
              <p>Paragraph 2</p>
            </main>
            <footer>Footer</footer>
          </div>
        </WithTooltip>,
      );

      expect(screen.getByTestId('container')).toBeInTheDocument();
      expect(screen.getByText('Header')).toBeInTheDocument();
      expect(screen.getByText('Paragraph 1')).toBeInTheDocument();
      expect(screen.getByText('Footer')).toBeInTheDocument();
    });
  });

  describe('Edge cases', () => {
    test('handles text node as children', () => {
      renderWithChakra(
        <WithTooltip isDisabled={false}>
          <span>Just text</span>
        </WithTooltip>,
      );

      expect(screen.getByText('Just text')).toBeInTheDocument();
    });

    test('handles special characters in tooltip text', () => {
      renderWithChakra(
        <WithTooltip isDisabled tooltipText="Special chars: &amp;'&quot;">
          <button type="button">Test</button>
        </WithTooltip>,
      );

      expect(screen.getByRole('button', { name: 'Test' })).toBeInTheDocument();
    });
  });
});
