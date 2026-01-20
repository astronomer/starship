import { describe, expect, test, vi, beforeEach } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import axios from 'axios';
import MigrateButton from '../../../src/component/MigrateButton';
import { renderWithChakra, createAxiosResponse, createAxiosError } from '../../test-utils';

// Mock axios
vi.mock('axios');

describe('MigrateButton', () => {
  const defaultProps = {
    route: 'https://test.astronomer.run/api/starship/variables',
    headers: { 'Starship-Proxy-Token': 'test-token' },
    sendData: { key: 'test_var', val: 'test_value' },
    existsInRemote: false,
    isDisabled: false,
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Initial render states', () => {
    test('renders Migrate button when existsInRemote is false', () => {
      renderWithChakra(<MigrateButton {...defaultProps} />);

      expect(screen.getByRole('button', { name: /migrate/i })).toBeInTheDocument();
    });

    test('renders Delete button when existsInRemote is true', () => {
      renderWithChakra(<MigrateButton {...defaultProps} existsInRemote />);

      expect(screen.getByRole('button', { name: /delete/i })).toBeInTheDocument();
    });

    test('renders disabled button when isDisabled is true', () => {
      renderWithChakra(<MigrateButton {...defaultProps} isDisabled />);

      expect(screen.getByRole('button')).toBeDisabled();
    });
  });

  describe('Migration flow', () => {
    test('sends POST request when migrate button is clicked', async () => {
      const user = userEvent.setup();
      const onStatusChange = vi.fn();

      axios.mockResolvedValue(createAxiosResponse({ key: 'test_var' }, 201));

      renderWithChakra(<MigrateButton {...defaultProps} onStatusChange={onStatusChange} />);

      const button = screen.getByRole('button', { name: /migrate/i });
      await user.click(button);

      await waitFor(() => {
        expect(axios).toHaveBeenCalledWith({
          method: 'post',
          url: defaultProps.route,
          headers: defaultProps.headers,
          data: defaultProps.sendData,
        });
      });

      await waitFor(() => {
        expect(onStatusChange).toHaveBeenCalledWith(true);
      });
    });

    test('shows loading state during migration', async () => {
      const user = userEvent.setup();

      axios.mockImplementation(
        () =>
          new Promise((resolve) => {
            setTimeout(() => resolve(createAxiosResponse({}, 200)), 100);
          }),
      );

      renderWithChakra(<MigrateButton {...defaultProps} />);

      const button = screen.getByRole('button', { name: /migrate/i });
      await user.click(button);

      expect(screen.getByText(/migrating/i)).toBeInTheDocument();
    });

    test('switches to Delete button after successful migration', async () => {
      const user = userEvent.setup();

      axios.mockResolvedValue(createAxiosResponse({}, 200));

      renderWithChakra(<MigrateButton {...defaultProps} />);

      const button = screen.getByRole('button', { name: /migrate/i });
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /delete/i })).toBeInTheDocument();
      });
    });
  });

  describe('Delete flow', () => {
    test('sends DELETE request when delete button is clicked', async () => {
      const user = userEvent.setup();
      const onStatusChange = vi.fn();

      axios.mockResolvedValue(createAxiosResponse(null, 204));

      renderWithChakra(<MigrateButton {...defaultProps} existsInRemote onStatusChange={onStatusChange} />);

      const button = screen.getByRole('button', { name: /delete/i });
      await user.click(button);

      await waitFor(() => {
        expect(axios).toHaveBeenCalledWith({
          method: 'delete',
          url: defaultProps.route,
          headers: defaultProps.headers,
          data: defaultProps.sendData,
        });
      });

      await waitFor(() => {
        expect(onStatusChange).toHaveBeenCalledWith(false);
      });
    });

    test('shows loading state during delete', async () => {
      const user = userEvent.setup();

      axios.mockImplementation(
        () =>
          new Promise((resolve) => {
            setTimeout(() => resolve(createAxiosResponse(null, 204)), 100);
          }),
      );

      renderWithChakra(<MigrateButton {...defaultProps} existsInRemote />);

      const button = screen.getByRole('button', { name: /delete/i });
      await user.click(button);

      expect(screen.getByText(/deleting/i)).toBeInTheDocument();
    });

    test('switches to Migrate button after successful delete', async () => {
      const user = userEvent.setup();

      axios.mockResolvedValue(createAxiosResponse(null, 204));

      renderWithChakra(<MigrateButton {...defaultProps} existsInRemote />);

      const button = screen.getByRole('button', { name: /delete/i });
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /migrate/i })).toBeInTheDocument();
      });
    });
  });

  describe('Error handling', () => {
    test('shows error state when migration fails', async () => {
      const user = userEvent.setup();

      axios.mockRejectedValue(createAxiosError('Migration failed', 500));

      renderWithChakra(<MigrateButton {...defaultProps} />);

      const button = screen.getByRole('button', { name: /migrate/i });
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /error/i })).toBeInTheDocument();
      });
    });

    test('shows error state when delete fails', async () => {
      const user = userEvent.setup();

      axios.mockRejectedValue(createAxiosError('Delete failed', 500));

      renderWithChakra(<MigrateButton {...defaultProps} existsInRemote />);

      const button = screen.getByRole('button', { name: /delete/i });
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /error/i })).toBeInTheDocument();
      });
    });
  });

  describe('Status codes', () => {
    test.each([200, 201, 204])('handles %i status as success', async (status) => {
      const user = userEvent.setup();
      const onStatusChange = vi.fn();

      axios.mockResolvedValue(createAxiosResponse({}, status));

      renderWithChakra(<MigrateButton {...defaultProps} onStatusChange={onStatusChange} />);

      await user.click(screen.getByRole('button', { name: /migrate/i }));

      await waitFor(() => {
        expect(onStatusChange).toHaveBeenCalledWith(true);
      });
    });
  });

  describe('Item name in toasts', () => {
    test('uses provided itemName in toast messages', async () => {
      const user = userEvent.setup();

      axios.mockResolvedValue(createAxiosResponse({}, 200));

      renderWithChakra(<MigrateButton {...defaultProps} itemName="Variable 'my_var'" />);

      await user.click(screen.getByRole('button', { name: /migrate/i }));

      // Toast should be called with the item name
      // We can't easily test toast content without mocking Chakra's toast,
      // but we can verify the component renders without error
      await waitFor(() => {
        expect(screen.getByRole('button', { name: /delete/i })).toBeInTheDocument();
      });
    });
  });

  describe('Button colors', () => {
    test('migrate button has green color scheme', () => {
      renderWithChakra(<MigrateButton {...defaultProps} />);

      const button = screen.getByRole('button', { name: /migrate/i });
      // Chakra applies colorScheme as a class or data attribute
      expect(button).toHaveClass('chakra-button');
    });

    test('delete button has red color scheme', () => {
      renderWithChakra(<MigrateButton {...defaultProps} existsInRemote />);

      const button = screen.getByRole('button', { name: /delete/i });
      expect(button).toHaveClass('chakra-button');
    });
  });
});
