import { describe, expect, test, vi, beforeEach, afterEach } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import axios from 'axios';
import DAGHistoryMigrateButton, { BUTTON_STATES } from '../../../src/component/DAGHistoryMigrateButton';
import { renderWithChakra, createAxiosResponse, createAxiosError } from '../../test-utils';

// Mock axios
vi.mock('axios');

describe('DAGHistoryMigrateButton', () => {
  const defaultProps = {
    url: 'https://test.astronomer.run/deployment',
    token: 'test-token',
    dagId: 'test_dag',
    limit: 100,
    batchSize: 10,
    existsInRemote: false,
    disabledReason: null,
  };

  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('BUTTON_STATES configuration', () => {
    test('BUTTON_STATES object is frozen', () => {
      expect(Object.isFrozen(BUTTON_STATES)).toBe(true);
    });

    test('MIGRATE state has correct properties', () => {
      expect(BUTTON_STATES.MIGRATE).toMatchObject({
        key: 'MIGRATE',
        buttonText: 'Migrate',
        color: 'success.600',
      });
    });

    test('DELETE state has correct properties', () => {
      expect(BUTTON_STATES.DELETE).toMatchObject({
        key: 'DELETE',
        buttonText: 'Delete',
        color: 'error.600',
      });
    });

    test('NOT_IN_REMOTE state has tooltip', () => {
      expect(BUTTON_STATES.NOT_IN_REMOTE.tooltip).toBe('Deploy DAG to remote before migrating');
    });

    test('NO_DAG_RUNS state has tooltip', () => {
      expect(BUTTON_STATES.NO_DAG_RUNS.tooltip).toBe('No DAG Runs to migrate');
    });
  });

  describe('Initial render states', () => {
    test('renders Migrate button when existsInRemote is false', () => {
      renderWithChakra(<DAGHistoryMigrateButton {...defaultProps} />);

      expect(screen.getByRole('button', { name: /migrate/i })).toBeInTheDocument();
    });

    test('renders Delete button when existsInRemote is true', () => {
      renderWithChakra(<DAGHistoryMigrateButton {...defaultProps} existsInRemote />);

      expect(screen.getByRole('button', { name: /delete/i })).toBeInTheDocument();
    });

    test('renders disabled button with NOT_IN_REMOTE state', () => {
      renderWithChakra(<DAGHistoryMigrateButton {...defaultProps} disabledReason={BUTTON_STATES.NOT_IN_REMOTE} />);

      const button = screen.getByRole('button', { name: /not on remote/i });
      expect(button).toBeInTheDocument();
      expect(button).toBeDisabled();
    });

    test('renders disabled button with NO_DAG_RUNS state', () => {
      renderWithChakra(<DAGHistoryMigrateButton {...defaultProps} disabledReason={BUTTON_STATES.NO_DAG_RUNS} />);

      const button = screen.getByRole('button', { name: /nothing to migrate/i });
      expect(button).toBeInTheDocument();
      expect(button).toBeDisabled();
    });
  });

  describe('Migration flow', () => {
    test('calls API endpoints when migrate button is clicked', async () => {
      const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
      const onMigrate = vi.fn();

      // Mock API responses
      axios.get.mockResolvedValue(
        createAxiosResponse({
          dag_runs: [{ dag_id: 'test_dag', run_id: 'run1' }],
          dag_run_count: 1,
          task_instances: [],
        }),
      );
      axios.post.mockResolvedValue(createAxiosResponse({ dag_run_count: 1 }));

      renderWithChakra(<DAGHistoryMigrateButton {...defaultProps} onMigrate={onMigrate} />);

      const button = screen.getByRole('button', { name: /migrate/i });
      await user.click(button);

      await waitFor(() => {
        expect(axios.get).toHaveBeenCalled();
      });

      // Advance timers to complete the action
      vi.advanceTimersByTime(600);

      await waitFor(() => {
        expect(axios.post).toHaveBeenCalled();
      });
    });

    test('shows progress during migration', async () => {
      const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });

      // Mock API responses with delay
      axios.get.mockImplementation(
        () =>
          new Promise((resolve) => {
            setTimeout(
              () =>
                resolve(
                  createAxiosResponse({
                    dag_runs: [{ dag_id: 'test_dag', run_id: 'run1' }],
                    dag_run_count: 1,
                    task_instances: [],
                  }),
                ),
              100,
            );
          }),
      );
      axios.post.mockResolvedValue(createAxiosResponse({ dag_run_count: 1 }));

      renderWithChakra(<DAGHistoryMigrateButton {...defaultProps} />);

      const button = screen.getByRole('button', { name: /migrate/i });
      await user.click(button);

      // Button should show migrating state
      await waitFor(() => {
        expect(screen.getByText(/migrating/i)).toBeInTheDocument();
      });
    });

    test('handles empty dag runs gracefully', async () => {
      const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
      const onMigrate = vi.fn();

      axios.get.mockResolvedValue(
        createAxiosResponse({
          dag_runs: [],
          dag_run_count: 0,
          task_instances: [],
        }),
      );

      renderWithChakra(<DAGHistoryMigrateButton {...defaultProps} onMigrate={onMigrate} />);

      const button = screen.getByRole('button', { name: /migrate/i });
      await user.click(button);

      vi.advanceTimersByTime(600);

      await waitFor(() => {
        expect(onMigrate).toHaveBeenCalledWith('test_dag', 0);
      });
    });
  });

  describe('Delete flow', () => {
    test('calls delete endpoint when delete button is clicked', async () => {
      const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
      const onDelete = vi.fn();

      axios.delete.mockResolvedValue(createAxiosResponse(null, 204));

      renderWithChakra(<DAGHistoryMigrateButton {...defaultProps} existsInRemote onDelete={onDelete} />);

      const button = screen.getByRole('button', { name: /delete/i });
      await user.click(button);

      await waitFor(() => {
        expect(axios.delete).toHaveBeenCalled();
      });

      vi.advanceTimersByTime(600);

      await waitFor(() => {
        expect(onDelete).toHaveBeenCalledWith('test_dag');
      });
    });

    test('shows deleting progress during delete operation', async () => {
      const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });

      axios.delete.mockImplementation(
        () =>
          new Promise((resolve) => {
            setTimeout(() => resolve(createAxiosResponse(null, 204)), 100);
          }),
      );

      renderWithChakra(<DAGHistoryMigrateButton {...defaultProps} existsInRemote />);

      const button = screen.getByRole('button', { name: /delete/i });
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByText(/deleting/i)).toBeInTheDocument();
      });
    });
  });

  describe('Error handling', () => {
    test('shows error state when migration fails', async () => {
      const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });

      axios.get.mockRejectedValue(createAxiosError('Network error', 500));

      renderWithChakra(<DAGHistoryMigrateButton {...defaultProps} />);

      const button = screen.getByRole('button', { name: /migrate/i });
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /error/i })).toBeInTheDocument();
      });
    });

    test('shows error state when delete fails', async () => {
      const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });

      axios.delete.mockRejectedValue(createAxiosError('Delete failed', 500));

      renderWithChakra(<DAGHistoryMigrateButton {...defaultProps} existsInRemote />);

      const button = screen.getByRole('button', { name: /delete/i });
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /error/i })).toBeInTheDocument();
      });
    });
  });

  describe('State synchronization', () => {
    test('syncs exists state with existsInRemote prop when not loading', () => {
      const { rerender } = renderWithChakra(<DAGHistoryMigrateButton {...defaultProps} existsInRemote={false} />);

      expect(screen.getByRole('button', { name: /migrate/i })).toBeInTheDocument();

      rerender(<DAGHistoryMigrateButton {...defaultProps} existsInRemote />);

      expect(screen.getByRole('button', { name: /delete/i })).toBeInTheDocument();
    });
  });

  describe('Button styling', () => {
    test('disabled buttons have correct disabled styling', () => {
      renderWithChakra(<DAGHistoryMigrateButton {...defaultProps} disabledReason={BUTTON_STATES.NOT_IN_REMOTE} />);

      const button = screen.getByRole('button');
      expect(button).toHaveAttribute('disabled');
    });

    test('button is disabled during loading', async () => {
      const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });

      axios.get.mockImplementation(
        () =>
          new Promise((resolve) => {
            setTimeout(
              () =>
                resolve(
                  createAxiosResponse({
                    dag_runs: [{ dag_id: 'test_dag' }],
                    dag_run_count: 1,
                    task_instances: [],
                  }),
                ),
              1000,
            );
          }),
      );

      renderWithChakra(<DAGHistoryMigrateButton {...defaultProps} />);

      const button = screen.getByRole('button', { name: /migrate/i });
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByRole('button')).toBeDisabled();
      });
    });
  });
});
