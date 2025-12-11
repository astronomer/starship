import React from 'react';
import PropTypes from 'prop-types';
import {
  Box,
  Card,
  CardBody,
  HStack,
  VStack,
  Text,
  Button,
  Progress,
  Stat,
  StatLabel,
  StatNumber,
  StatHelpText,
  Icon,
} from '@chakra-ui/react';
import { CheckCircleIcon } from '@chakra-ui/icons';
import { FiUpload } from 'react-icons/fi';

export default function ProgressSummary({
  totalItems, migratedItems, onMigrateAll, isMigratingAll,
}) {
  const remainingItems = totalItems - migratedItems;
  const progressPercentage = totalItems > 0 ? (migratedItems / totalItems) * 100 : 0;
  const isComplete = migratedItems === totalItems && totalItems > 0;

  return (
    <Card mb={4} bg="brand.50" borderWidth={2} borderColor="brand.300">
      <CardBody py={3}>
        <HStack spacing={6} align="center">
          {/* Progress Stats */}
          <HStack spacing={6} flex={1}>
            <Stat>
              <StatLabel fontSize="xs" color="gray.600">
                Total Items
              </StatLabel>
              <StatNumber fontSize="2xl">{totalItems}</StatNumber>
            </Stat>

            <Stat>
              <StatLabel fontSize="xs" color="gray.600">
                Migrated
              </StatLabel>
              <StatNumber fontSize="2xl" color="success.600">
                {migratedItems}
              </StatNumber>
              <StatHelpText fontSize="xs" mb={0}>
                {progressPercentage.toFixed(0)}
                % complete
              </StatHelpText>
            </Stat>

            <Stat>
              <StatLabel fontSize="xs" color="gray.600">
                Remaining
              </StatLabel>
              <StatNumber fontSize="2xl" color={remainingItems > 0 ? 'orange.600' : 'gray.400'}>
                {remainingItems}
              </StatNumber>
            </Stat>
          </HStack>

          {/* Progress Bar */}
          <VStack flex={1} align="stretch" spacing={1}>
            <Text fontSize="xs" fontWeight="semibold" color="gray.700">
              Migration Progress
            </Text>
            <Progress
              value={progressPercentage}
              size="lg"
              colorScheme={isComplete ? 'success' : 'brand'}
              borderRadius="md"
              hasStripe={isMigratingAll}
              isAnimated={isMigratingAll}
            />
            <Text fontSize="2xs" color="gray.600">
              {isComplete ? (
                <HStack spacing={1}>
                  <Icon as={CheckCircleIcon} color="green.500" />
                  <Text>All items migrated successfully!</Text>
                </HStack>
              ) : (
                `${remainingItems} item${remainingItems !== 1 ? 's' : ''} to migrate`
              )}
            </Text>
          </VStack>

          {/* Migrate All Button */}
          <Box>
            <Button
              size="sm"
              variant="outline"
              leftIcon={<Icon as={FiUpload} />}
              colorScheme="brand"
              isDisabled={remainingItems === 0 || isMigratingAll}
              isLoading={isMigratingAll}
              loadingText="Migrating..."
              onClick={onMigrateAll}
            >
              Migrate All
            </Button>
          </Box>
        </HStack>
      </CardBody>
    </Card>
  );
}

ProgressSummary.propTypes = {
  totalItems: PropTypes.number.isRequired,
  migratedItems: PropTypes.number.isRequired,
  onMigrateAll: PropTypes.func.isRequired,
  isMigratingAll: PropTypes.bool,
};

ProgressSummary.defaultProps = {
  isMigratingAll: false,
};
