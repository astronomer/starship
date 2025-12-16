import React from 'react';
import PropTypes from 'prop-types';
import {
  Box,
  Card,
  CardBody,
  Collapse,
  Heading,
  HStack,
  Tooltip,
  VStack,
} from '@chakra-ui/react';
import { ChevronDownIcon, QuestionIcon } from '@chakra-ui/icons';

/**
 * Reusable step card component for the setup wizard.
 * Provides consistent styling for collapsible setup steps.
 */
export default function SetupStep({
  stepNumber,
  title,
  tooltip,
  isComplete,
  showCheck,
  isOpen,
  onToggle,
  isDisabled = false,
  children,
}) {
  const canToggle = isComplete && !isDisabled;

  return (
    <Card
      opacity={isDisabled ? 0.5 : 1}
      pointerEvents={isDisabled ? 'none' : 'auto'}
      transition="all 0.3s"
    >
      <CardBody py={3}>
        <VStack align="stretch" spacing={2}>
          <HStack
            mb={1}
            justify="space-between"
            cursor={canToggle ? 'pointer' : 'default'}
            onClick={canToggle ? onToggle : undefined}
            _hover={canToggle ? { bg: 'gray.50' } : undefined}
            transition="background 0.2s"
            borderRadius="md"
            px={2}
            py={1}
            mx={-2}
          >
            <HStack>
              <Box
                display="inline-flex"
                alignItems="center"
                justifyContent="center"
                w={6}
                h={6}
                borderRadius="full"
                bg={isComplete ? 'success.500' : 'brand.500'}
                color="white"
                fontWeight="bold"
                fontSize="xs"
                mr={2}
                transition="all 0.3s"
              >
                {isComplete && showCheck ? '✓' : stepNumber}
              </Box>
              <Heading size="sm">{title}</Heading>
              <Tooltip label={tooltip} placement="top" hasArrow>
                <QuestionIcon color="gray.400" boxSize={3} cursor="help" />
              </Tooltip>
            </HStack>
            {isComplete && (
              <ChevronDownIcon
                boxSize={4}
                transform={isOpen ? 'rotate(180deg)' : 'rotate(0deg)'}
                transition="transform 0.2s"
              />
            )}
          </HStack>
          <Collapse in={isOpen} animateOpacity>
            {children}
          </Collapse>
        </VStack>
      </CardBody>
    </Card>
  );
}

SetupStep.propTypes = {
  stepNumber: PropTypes.oneOfType([PropTypes.string, PropTypes.number]).isRequired,
  title: PropTypes.string.isRequired,
  tooltip: PropTypes.string.isRequired,
  isComplete: PropTypes.bool.isRequired,
  showCheck: PropTypes.bool.isRequired,
  isOpen: PropTypes.bool.isRequired,
  onToggle: PropTypes.func.isRequired,
  isDisabled: PropTypes.bool,
  children: PropTypes.node.isRequired,
};
