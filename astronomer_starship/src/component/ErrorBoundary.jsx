import React from 'react';
import PropTypes from 'prop-types';
import {
  Alert,
  AlertDescription,
  AlertIcon,
  AlertTitle,
  Box,
  Button,
  Code,
  Collapse,
  useDisclosure,
  VStack,
} from '@chakra-ui/react';

/**
 * Error Boundary component to catch and display React errors gracefully
 */
class ErrorBoundaryClass extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null, errorInfo: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, errorInfo) {
    this.setState({ errorInfo });
    // Log to console in development
    if (process.env.NODE_ENV === 'development') {
      // eslint-disable-next-line no-console
      console.error('ErrorBoundary caught an error:', error, errorInfo);
    }
  }

  render() {
    const { hasError, error, errorInfo } = this.state;
    const { children, fallback } = this.props;

    if (hasError) {
      if (fallback) {
        return fallback;
      }

      return (
        <ErrorFallback
          error={error}
          errorInfo={errorInfo}
          onReset={() => this.setState({ hasError: false, error: null, errorInfo: null })}
        />
      );
    }

    return children;
  }
}

ErrorBoundaryClass.propTypes = {
  children: PropTypes.node.isRequired,
  fallback: PropTypes.node,
};

ErrorBoundaryClass.defaultProps = {
  fallback: null,
};

/**
 * Fallback UI component shown when an error occurs
 */
function ErrorFallback({ error = null, errorInfo = null, onReset }) {
  const { isOpen, onToggle } = useDisclosure();

  return (
    <Box p={4} maxW="3xl" mx="auto" mt={8}>
      <Alert
        status="error"
        variant="subtle"
        flexDirection="column"
        alignItems="flex-start"
        borderRadius="md"
        p={6}
      >
        <AlertIcon boxSize="40px" mr={0} mb={4} />
        <AlertTitle fontSize="lg" mb={2}>
          Something went wrong
        </AlertTitle>
        <AlertDescription mb={4}>
          An unexpected error occurred. Please try refreshing the page.
        </AlertDescription>

        <VStack align="stretch" spacing={3} w="100%">
          <Button colorScheme="red" variant="outline" onClick={onReset}>
            Try Again
          </Button>
          <Button size="sm" variant="ghost" onClick={onToggle}>
            {isOpen ? 'Hide' : 'Show'}
            {' '}
            Error Details
          </Button>
          <Collapse in={isOpen}>
            <Box
              bg="gray.50"
              p={3}
              borderRadius="md"
              fontSize="sm"
              overflow="auto"
              maxH="xs"
            >
              <Code display="block" whiteSpace="pre-wrap" bg="transparent">
                {error?.toString()}
                {errorInfo?.componentStack}
              </Code>
            </Box>
          </Collapse>
        </VStack>
      </Alert>
    </Box>
  );
}

ErrorFallback.propTypes = {
  error: PropTypes.instanceOf(Error),
  errorInfo: PropTypes.shape({
    componentStack: PropTypes.string,
  }),
  onReset: PropTypes.func.isRequired,
};

export default ErrorBoundaryClass;
