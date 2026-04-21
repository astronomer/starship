import PropTypes from 'prop-types';
import { Box, HStack, Radio, RadioGroup, Text, VStack } from '@chakra-ui/react';
import { SOURCE_PLATFORMS } from '../constants';

/**
 * Radio-card picker for the source Airflow platform.
 * Selecting a platform swaps in the matching credential fields below.
 */
export default function SourcePlatformPicker({ value = null, onChange }) {
  return (
    <RadioGroup value={value || ''} onChange={onChange}>
      <VStack align="stretch" spacing={2}>
        {SOURCE_PLATFORMS.map((p) => {
          const isSelected = value === p.id;
          return (
            <Box
              key={p.id}
              as="label"
              htmlFor={`source-platform-${p.id}`}
              borderWidth="1px"
              borderColor={isSelected ? 'brand.500' : 'gray.200'}
              bg={isSelected ? 'brand.50' : 'white'}
              borderRadius="md"
              px={3}
              py={2}
              cursor="pointer"
              transition="all 0.15s"
              _hover={{ borderColor: 'brand.400' }}
            >
              <HStack align="flex-start" spacing={3}>
                <Radio id={`source-platform-${p.id}`} value={p.id} mt={1} />
                <Box>
                  <Text fontWeight="semibold" fontSize="sm">
                    {p.label}
                  </Text>
                  <Text fontSize="xs" color="gray.600">
                    {p.description}
                  </Text>
                </Box>
              </HStack>
            </Box>
          );
        })}
      </VStack>
    </RadioGroup>
  );
}

SourcePlatformPicker.propTypes = {
  value: PropTypes.string,
  onChange: PropTypes.func.isRequired,
};
