import { useCallback, useState } from 'react';

/**
 * State hook for the reusable {@link ConfirmDialog} component.
 *
 * Returns ``[confirmProps, ask]`` — spread ``confirmProps`` into
 * ``<ConfirmDialog>``, and call ``ask(...)`` to open it:
 *
 *   const [confirmProps, ask] = useConfirm();
 *   ask({
 *     title: 'Roll back wave?',
 *     body: 'This deletes migrated metadata on this Airflow.',
 *     confirmLabel: 'Roll back',
 *     colorScheme: 'red',
 *     onConfirm: async () => { ... },
 *   });
 *
 * ``onConfirm`` can be async — the dialog shows a spinner on the confirm
 * button until the returned promise resolves, then auto-closes. Throwing
 * from ``onConfirm`` leaves the dialog open so the caller can surface the
 * error via toast and let the user decide what to do.
 */
export default function useConfirm() {
  const [state, setState] = useState({
    isOpen: false,
    title: '',
    body: '',
    confirmLabel: 'Confirm',
    cancelLabel: 'Cancel',
    colorScheme: 'red',
    onConfirm: null,
    isLoading: false,
  });

  const close = useCallback(() => {
    setState((s) => ({ ...s, isOpen: false, isLoading: false }));
  }, []);

  const ask = useCallback((opts) => {
    setState({
      isOpen: true,
      title: opts.title ?? 'Are you sure?',
      body: opts.body ?? '',
      confirmLabel: opts.confirmLabel ?? 'Confirm',
      cancelLabel: opts.cancelLabel ?? 'Cancel',
      colorScheme: opts.colorScheme ?? 'red',
      onConfirm: opts.onConfirm,
      isLoading: false,
    });
  }, []);

  const handleConfirm = useCallback(async () => {
    const cb = state.onConfirm;
    if (!cb) {
      close();
      return;
    }
    setState((s) => ({ ...s, isLoading: true }));
    try {
      await cb();
      close();
    } catch {
      // Caller is responsible for toast / error surfacing.
      // Keep the dialog open but drop the spinner so they can retry or cancel.
      setState((s) => ({ ...s, isLoading: false }));
    }
  }, [state.onConfirm, close]);

  const confirmProps = {
    isOpen: state.isOpen,
    title: state.title,
    body: state.body,
    confirmLabel: state.confirmLabel,
    cancelLabel: state.cancelLabel,
    colorScheme: state.colorScheme,
    isLoading: state.isLoading,
    onConfirm: handleConfirm,
    onClose: close,
  };

  return [confirmProps, ask];
}
