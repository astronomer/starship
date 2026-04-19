import { Box, Card, CardBody, Heading, Text, VStack } from '@chakra-ui/react';
import { useSourceConfig } from '../AppContext';

/**
 * Cutover Tool — placeholder.
 *
 * Next iterations wire in:
 *  - Wave config form (big-bang / incremental)
 *  - Wave status and history
 *  - Rollback / retry / abort actions
 *
 * This page is only reachable once the source-setup step is complete.
 */
export default function CutoverPage() {
  const source = useSourceConfig();

  return (
    <Box>
      <Heading size="md" mb={1}>
        Cutover
      </Heading>
      <Text fontSize="xs" color="gray.600" mb={4}>
        Migrate DAG metadata from your source Airflow into this one, in waves, with rollback.
      </Text>
      <Card>
        <CardBody>
          <VStack align="stretch" spacing={2}>
            <Text fontSize="sm">
              Source connection <strong>starship_source</strong> is configured.
            </Text>
            <Text fontSize="xs" color="gray.600">
              Platform: <strong>{source.platform}</strong>
              <br />
              URL: <strong>{source.url}</strong>
            </Text>
            <Text fontSize="xs" color="gray.500" fontStyle="italic" mt={2}>
              Wave configuration and status UI will be added in a follow-up commit.
            </Text>
          </VStack>
        </CardBody>
      </Card>
    </Box>
  );
}
