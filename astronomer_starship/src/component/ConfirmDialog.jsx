import { useRef } from 'react';
import PropTypes from 'prop-types';
import {
  AlertDialog,
  AlertDialogBody,
  AlertDialogContent,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogOverlay,
  Button,
} from '@chakra-ui/react';

/**
 * Reusable AlertDialog-based confirm for destructive / irreversible actions.
 *
 * Usage:
 *   const [confirmProps, ask] = useConfirm();
 *   <ConfirmDialog {...confirmProps} />
 *   ...
 *   ask({ title, body, confirmLabel, onConfirm }) // triggers the dialog
 */
export default function ConfirmDialog({
  isOpen,
  title,
  body,
  confirmLabel,
  cancelLabel,
  colorScheme,
  isLoading,
  onConfirm,
  onClose,
}) {
  const cancelRef = useRef(null);
  return (
    <AlertDialog isOpen={isOpen} leastDestructiveRef={cancelRef} onClose={onClose}>
      <AlertDialogOverlay>
        <AlertDialogContent>
          <AlertDialogHeader fontSize="lg" fontWeight="bold">
            {title}
          </AlertDialogHeader>
          <AlertDialogBody>{body}</AlertDialogBody>
          <AlertDialogFooter>
            <Button ref={cancelRef} onClick={onClose} size="sm" variant="outline" isDisabled={isLoading}>
              {cancelLabel}
            </Button>
            <Button colorScheme={colorScheme} onClick={onConfirm} ml={3} size="sm" isLoading={isLoading}>
              {confirmLabel}
            </Button>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialogOverlay>
    </AlertDialog>
  );
}

ConfirmDialog.propTypes = {
  isOpen: PropTypes.bool.isRequired,
  title: PropTypes.node.isRequired,
  body: PropTypes.node.isRequired,
  confirmLabel: PropTypes.string,
  cancelLabel: PropTypes.string,
  colorScheme: PropTypes.string,
  isLoading: PropTypes.bool,
  onConfirm: PropTypes.func.isRequired,
  onClose: PropTypes.func.isRequired,
};

ConfirmDialog.defaultProps = {
  confirmLabel: 'Confirm',
  cancelLabel: 'Cancel',
  colorScheme: 'red',
  isLoading: false,
};
