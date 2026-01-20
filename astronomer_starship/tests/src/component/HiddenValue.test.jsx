import { describe, expect, test } from 'vitest';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import HiddenValue from '../../../src/component/HiddenValue';
import { renderWithChakra } from '../../test-utils';

describe('HiddenValue', () => {
  describe('Rendering', () => {
    test('renders nothing visible when value is empty string', () => {
      renderWithChakra(<HiddenValue value="" />);

      // No button should be rendered when value is empty
      expect(screen.queryByRole('button')).not.toBeInTheDocument();
    });

    test('renders nothing visible when value is undefined', () => {
      renderWithChakra(<HiddenValue />);

      // No button should be rendered
      expect(screen.queryByRole('button')).not.toBeInTheDocument();
    });

    test('renders input group when value is provided', () => {
      renderWithChakra(<HiddenValue value="secret-value" />);

      // Should have Show button
      expect(screen.getByRole('button', { name: /show/i })).toBeInTheDocument();
    });

    test('input is read-only', () => {
      const { container } = renderWithChakra(<HiddenValue value="secret-value" />);

      // Find the input element directly
      const input = container.querySelector('input');
      expect(input).toHaveAttribute('readonly');
    });
  });

  describe('Password visibility toggle', () => {
    test('initially hides the value (password type)', () => {
      const { container } = renderWithChakra(<HiddenValue value="secret-value" />);

      const input = container.querySelector('input');
      expect(input).toHaveAttribute('type', 'password');
    });

    test('shows value when Show button is clicked', async () => {
      const user = userEvent.setup();
      const { container } = renderWithChakra(<HiddenValue value="secret-value" />);

      await user.click(screen.getByRole('button', { name: /show/i }));

      const input = container.querySelector('input');
      expect(input).toHaveAttribute('type', 'text');
    });

    test('hides value when Hide button is clicked', async () => {
      const user = userEvent.setup();
      const { container } = renderWithChakra(<HiddenValue value="secret-value" />);

      // First show the value
      await user.click(screen.getByRole('button', { name: /show/i }));

      // Then hide it
      await user.click(screen.getByRole('button', { name: /hide/i }));

      const input = container.querySelector('input');
      expect(input).toHaveAttribute('type', 'password');
    });

    test('button text changes between Show and Hide', async () => {
      const user = userEvent.setup();

      renderWithChakra(<HiddenValue value="secret-value" />);

      // Initially shows "Show" button
      expect(screen.getByRole('button', { name: /show/i })).toBeInTheDocument();

      await user.click(screen.getByRole('button', { name: /show/i }));

      // After click, shows "Hide" button
      expect(screen.getByRole('button', { name: /hide/i })).toBeInTheDocument();
    });
  });

  describe('Value display', () => {
    test('displays the provided value in the input', async () => {
      const user = userEvent.setup();
      const { container } = renderWithChakra(<HiddenValue value="my-secret-password" />);

      // Show the value first
      await user.click(screen.getByRole('button', { name: /show/i }));

      const input = container.querySelector('input');
      expect(input).toHaveValue('my-secret-password');
    });

    test('handles special characters in value', async () => {
      const user = userEvent.setup();
      const { container } = renderWithChakra(<HiddenValue value="p@ssw0rd!#$%^&*()" />);

      await user.click(screen.getByRole('button', { name: /show/i }));

      const input = container.querySelector('input');
      expect(input).toHaveValue('p@ssw0rd!#$%^&*()');
    });

    test('handles very long values', () => {
      const longValue = 'a'.repeat(1000);
      const { container } = renderWithChakra(<HiddenValue value={longValue} />);

      const input = container.querySelector('input');
      expect(input).toHaveValue(longValue);
    });
  });

  describe('Edge cases', () => {
    test('handles whitespace-only value', () => {
      renderWithChakra(<HiddenValue value="   " />);

      // Whitespace is still a truthy value, so it should render
      expect(screen.getByRole('button', { name: /show/i })).toBeInTheDocument();
    });

    test('handles value with special unicode', async () => {
      const user = userEvent.setup();
      const unicodeValue = 'unicode: éñü';
      const { container } = renderWithChakra(<HiddenValue value={unicodeValue} />);

      await user.click(screen.getByRole('button', { name: /show/i }));

      const input = container.querySelector('input');
      expect(input).toHaveValue(unicodeValue);
    });
  });
});
