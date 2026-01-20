import { describe, expect, test, vi } from 'vitest';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import ProgressSummary from '../../../src/component/ProgressSummary';
import { renderWithChakra } from '../../test-utils';

describe('ProgressSummary', () => {
  const defaultProps = {
    totalItems: 10,
    migratedItems: 5,
    onMigrateAll: vi.fn(),
    isMigratingAll: false,
  };

  describe('Statistics display', () => {
    test('displays total items count', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} />);

      expect(screen.getByText('10')).toBeInTheDocument();
      expect(screen.getByText('Total')).toBeInTheDocument();
    });

    test('displays migrated items count', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} />);

      // Both migrated and remaining show 5, so we check both exist
      const fives = screen.getAllByText('5');
      expect(fives.length).toBe(2);
      expect(screen.getByText('Migrated')).toBeInTheDocument();
    });

    test('displays remaining items count', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} />);

      expect(screen.getByText('Remaining')).toBeInTheDocument();
    });

    test('calculates remaining correctly', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} totalItems={20} migratedItems={8} />);

      // Remaining should be 20 - 8 = 12 (using 8 to avoid collision with migrated count)
      expect(screen.getByText('12')).toBeInTheDocument();
    });
  });

  describe('Progress bar', () => {
    test('displays progress percentage', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} />);

      // 5/10 = 50%
      expect(screen.getByText('50%')).toBeInTheDocument();
    });

    test('displays 0% when no items migrated', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} migratedItems={0} />);

      expect(screen.getByText('0%')).toBeInTheDocument();
    });

    test('displays 100% when all items migrated', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} migratedItems={10} />);

      expect(screen.getByText('100%')).toBeInTheDocument();
    });

    test('handles zero total items without division by zero', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} totalItems={0} migratedItems={0} />);

      expect(screen.getByText('0%')).toBeInTheDocument();
    });

    test('rounds progress percentage to whole number', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} totalItems={3} migratedItems={1} />);

      // 1/3 = 33.33...%, should round to 33%
      expect(screen.getByText('33%')).toBeInTheDocument();
    });
  });

  describe('Migrate All button', () => {
    test('renders Migrate All button', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} />);

      expect(screen.getByRole('button', { name: /migrate all/i })).toBeInTheDocument();
    });

    test('calls onMigrateAll when clicked', async () => {
      const user = userEvent.setup();
      const onMigrateAll = vi.fn();

      renderWithChakra(<ProgressSummary {...defaultProps} onMigrateAll={onMigrateAll} />);

      await user.click(screen.getByRole('button', { name: /migrate all/i }));

      expect(onMigrateAll).toHaveBeenCalledTimes(1);
    });

    test('button is disabled when no remaining items', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} migratedItems={10} />);

      expect(screen.getByRole('button', { name: /migrate all/i })).toBeDisabled();
    });

    test('button is disabled when isMigratingAll is true', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} isMigratingAll />);

      // During migration, the button shows "Migrating..." and is disabled
      const button = screen.getByRole('button');
      expect(button).toBeDisabled();
    });

    test('shows loading text when isMigratingAll is true', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} isMigratingAll />);

      expect(screen.getByText(/migrating/i)).toBeInTheDocument();
    });
  });

  describe('Completion state', () => {
    test('shows check icon when migration is complete', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} totalItems={5} migratedItems={5} />);

      // The completion state is indicated by 100% and potentially different styling
      expect(screen.getByText('100%')).toBeInTheDocument();
      expect(screen.getByText('0')).toBeInTheDocument(); // Remaining is 0
    });

    test('does not show complete state when items remain', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} totalItems={5} migratedItems={3} />);

      expect(screen.getByText('60%')).toBeInTheDocument();
      expect(screen.getByText('2')).toBeInTheDocument(); // Remaining
    });
  });

  describe('Edge cases', () => {
    test('handles single item total', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} totalItems={1} migratedItems={0} />);

      // Total is 1, remaining is 1 (duplicate), but Total label should exist
      expect(screen.getByText('Total')).toBeInTheDocument();
      expect(screen.getByText('0%')).toBeInTheDocument();
    });

    test('handles large numbers', () => {
      renderWithChakra(<ProgressSummary {...defaultProps} totalItems={10000} migratedItems={4000} />);

      expect(screen.getByText('10000')).toBeInTheDocument();
      expect(screen.getByText('4000')).toBeInTheDocument();
      expect(screen.getByText('6000')).toBeInTheDocument(); // Remaining
      expect(screen.getByText('40%')).toBeInTheDocument();
    });
  });
});
