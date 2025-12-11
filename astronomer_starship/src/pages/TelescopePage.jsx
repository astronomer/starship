import React, { useState, useMemo } from 'react';
import {
  Box,
  Button,
  CircularProgress,
  FormControl,
  FormErrorMessage,
  FormHelperText,
  FormLabel,
  HStack,
  Heading,
  Input,
  InputGroup,
  Link,
  Text,
  Tooltip,
  VStack,
  useToast,
  Stack,
} from '@chakra-ui/react';
import { GoDownload, GoUpload } from 'react-icons/go';
import { RepeatIcon } from '@chakra-ui/icons';
import axios from 'axios';
import { FaCheck } from 'react-icons/fa';

import { useAppState, useAppDispatch } from '../AppContext';
import constants from '../constants';
import { localRoute } from '../util';

export default function TelescopePage() {
  const { telescopeOrganizationId, telescopePresignedUrl } = useAppState();
  const dispatch = useAppDispatch();

  const [isUploading, setIsUploading] = useState(false);
  const [isUploadComplete, setIsUploadComplete] = useState(false);
  const [error, setError] = useState(null);
  const toast = useToast();

  const route = useMemo(() => {
    const baseRoute = localRoute(constants.TELESCOPE_ROUTE);
    if (telescopeOrganizationId) {
      const params = new URLSearchParams({ organization: telescopeOrganizationId });
      if (telescopePresignedUrl) {
        params.set('presigned_url', telescopePresignedUrl);
      }
      return `${baseRoute}?${params.toString()}`;
    }
    return baseRoute;
  }, [telescopeOrganizationId, telescopePresignedUrl]);

  const filename = useMemo(() => (
    `${telescopeOrganizationId}.${new Date().toISOString().slice(0, 10)}.data.json`
  ), [telescopeOrganizationId]);

  const handleUpload = async () => {
    setIsUploading(true);
    setError(null);
    try {
      await axios.get(route);
      setIsUploadComplete(true);
    } catch (err) {
      toast({
        title: err.response?.data?.error || err.response?.data || err.message,
        status: 'error',
        isClosable: true,
      });
      setError(err);
    } finally {
      setIsUploading(false);
    }
  };

  const handleReset = () => {
    dispatch({ type: 'set-telescope-org', telescopeOrganizationId: '' });
    dispatch({ type: 'set-telescope-presigned-url', telescopePresignedUrl: '' });
    setIsUploadComplete(false);
    setError(null);
  };

  /* eslint-disable no-nested-ternary */
  return (
    <Box>
      <Stack
        direction={{ base: 'column', md: 'row' }}
        justify="space-between"
        align={{ base: 'flex-start', md: 'center' }}
        mb={3}
      >
        <Box>
          <Heading size="md" mb={0.5}>Telescope</Heading>
          <Text fontSize="xs" color="gray.600">
            Gather metadata from an Airflow instance to collect insights.
          </Text>
        </Box>
        <HStack>
          <Button
            size="sm"
            leftIcon={<RepeatIcon />}
            onClick={handleReset}
            variant="outline"
          >
            Reset
          </Button>
        </HStack>
      </Stack>

      <VStack spacing={3} align="stretch" w="100%">
        <Box maxW="600px">
          <FormControl marginY="2%">
            <FormLabel htmlFor="telescopeOrg" fontSize="sm" fontWeight="semibold" mb={1}>
              Organization
            </FormLabel>
            <InputGroup size="sm">
              <Input
                size="sm"
                id="telescopeOrg"
                placeholder="Astronomer, LLC"
                value={telescopeOrganizationId}
                onChange={(e) => dispatch({
                  type: 'set-telescope-org',
                  telescopeOrganizationId: e.target.value,
                })}
              />
            </InputGroup>
            <FormHelperText fontSize="xs">
              The ID of the Astronomer Organization to associate with this report.
            </FormHelperText>
          </FormControl>

          <FormControl mb={2}>
            <FormLabel htmlFor="telescopePresignedUrl" fontSize="sm" fontWeight="semibold" mb={1}>
              Pre-signed URL
            </FormLabel>
            <InputGroup size="sm">
              <Input
                size="sm"
                id="telescopePresignedUrl"
                placeholder="https://storage.googleapis.com/astronomer-telescope/..."
                value={telescopePresignedUrl}
                onChange={(e) => dispatch({
                  type: 'set-telescope-presigned-url',
                  telescopePresignedUrl: e.target.value,
                })}
              />
            </InputGroup>
            <FormHelperText fontSize="xs">
              (Optional) Enter a pre-signed URL to submit the report.
            </FormHelperText>
            <FormErrorMessage>Please fill both parts.</FormErrorMessage>
          </FormControl>

          <HStack spacing={2}>
            <Tooltip hasArrow label="Upload the report via pre-signed url to Astronomer">
              <Button
                size="sm"
                variant="outline"
                leftIcon={isUploading ? <span /> : isUploadComplete ? <FaCheck /> : <GoUpload />}
                isDisabled={isUploading || isUploadComplete || !telescopePresignedUrl || !telescopeOrganizationId}
                colorScheme={isUploadComplete ? 'green' : error ? 'red' : 'gray'}
                onClick={handleUpload}
              >
                {isUploading ? (
                  <CircularProgress thickness="20px" size="30px" isIndeterminate />
                ) : isUploadComplete ? (
                  'Done'
                ) : error ? (
                  'Error!'
                ) : (
                  'Upload'
                )}
              </Button>
            </Tooltip>

            <Tooltip hasArrow label="Download the report">
              <Button
                leftIcon={<GoDownload />}
                isDisabled={!telescopeOrganizationId}
                href={route}
                as={Link}
                download={filename}
                target="_blank"
              >
                Download
              </Button>
            </Tooltip>
          </HStack>
        </Box>
      </VStack>
    </Box>
  );
  /* eslint-enable no-nested-ternary */
}
