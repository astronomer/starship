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
  Icon,
  Spacer,
} from '@chakra-ui/react';
import { CheckCircleIcon } from '@chakra-ui/icons';
import { FiUpload } from 'react-icons/fi';

function StatFeature({ label, value, color }) {
  return (
    <VStack spacing={0} align="center" h="full" minW="70px">
      <Text fontSize="xs" color="gray.500" textTransform="uppercase" letterSpacing="wide">
        {label}
      </Text>
      <Spacer />
      <Text fontSize="2xl" fontWeight="bold" color={color} lineHeight="1">
        {value}
      </Text>
    </VStack>
  );
}

StatFeature.propTypes = {
  label: PropTypes.string.isRequired,
  value: PropTypes.oneOfType([PropTypes.string, PropTypes.number]).isRequired,
  color: PropTypes.string,
};

StatFeature.defaultProps = {
  color: 'gray.700',
};

export default function ProgressSummary({
  totalItems, migratedItems, onMigrateAll, isMigratingAll,
}) {
  const remainingItems = totalItems - migratedItems;
  const progressPercentage = totalItems > 0 ? (migratedItems / totalItems) * 100 : 0;
  const isComplete = migratedItems === totalItems && totalItems > 0;

  return (
    <Card mb={4} bg="brand.10" borderWidth={1} borderColor="brand.100">
      <CardBody py={4}>
        <HStack spacing={0} align="stretch">
          {/* Progress Stats */}
          <HStack spacing={0} align="stretch" divider={<Box w="1px" bg="gray.200" />}>
            <Box px={4}>
              <StatFeature label="Total" value={totalItems} />
            </Box>
            <Box px={4}>
              <StatFeature label="Migrated" value={migratedItems} color="success.600" />
            </Box>
            <Box px={4}>
              <StatFeature
                label="Remaining"
                value={remainingItems}
                color={remainingItems > 0 ? 'orange.500' : 'gray.400'}
              />
            </Box>
          </HStack>

          <Spacer />

          {/* Progress Bar + Button */}
          <HStack flex={1} spacing={6} align="center">
            <VStack flex={1} align="stretch" spacing={1}>
              <HStack justify="space-between" align="center">
                <Text fontSize="xs" color="gray.600" textTransform="uppercase" letterSpacing="wide">
                  Migration Progress
                </Text>
                <HStack spacing={1}>
                  <Text fontSize="xs" color="gray.500">
                    {`${progressPercentage.toFixed(0)}%`}
                  </Text>
                  <Text fontSize="xs" color="gray.600">{isComplete ? <Icon as={CheckCircleIcon} color="green.500" /> : null}</Text>
                </HStack>
              </HStack>
              <Progress
                value={progressPercentage}
                size="md"
                colorScheme={isComplete ? 'success' : 'brand'}
                borderRadius="full"
                bg="gray.100"
                hasStripe={isMigratingAll}
                isAnimated={isMigratingAll}
              />
            </VStack>
            <Button
              size="sm"
              variant="outline"
              leftIcon={<Icon as={FiUpload} boxSize={4} />}
              colorScheme="brand"
              isDisabled={remainingItems === 0 || isMigratingAll}
              isLoading={isMigratingAll}
              loadingText="Migrating..."
              onClick={onMigrateAll}
            >
              Migrate All
            </Button>
          </HStack>
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
