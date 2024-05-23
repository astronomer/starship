import {
  Box, Button,
  Divider, FormControl,
  FormErrorMessage,
  FormHelperText,
  FormLabel, Link,
  Input, InputGroup,
  Text, Tooltip, VStack, CircularProgress, useToast,
} from '@chakra-ui/react';
import React, { useEffect, useMemo, useState } from 'react';
import { GoDownload, GoUpload } from 'react-icons/go';
import axios from "axios";
import constants from "../constants.js";
import { localRoute } from "../util.js";
import { FaCheck } from "react-icons/fa";


export default function TelescopePage({ state, dispatch }) {
  const [isUploading, setIsUploading] = useState(false);
  const [isUploadComplete, setIsUploadComplete] = useState(false);
  const toast = useToast();
  const [route, setRoute] = useState('');
  const [filename, setFilename] = useState('');
  const [error, setError] = useState(null);
  useEffect(() => {
    const _route = localRoute(
      constants.TELESCOPE_ROUTE +
      (
        state.telescopeOrganizationId ? (
          `?organization=${state.telescopeOrganizationId}` +
          (state.telescopePresignedUrl ? `&presigned_url=${encodeURIComponent(state.telescopePresignedUrl)}` : '')
        ) : ''
      )
    )
    setRoute(_route);
    const _filename = `${state.telescopeOrganizationId}.${(new Date()).toISOString().slice(0,10)}.data.json`
    setFilename(_filename);
    console.log(_route, _filename);
  }, [state]);
  return (
    <Box>
      <Text fontSize="xl">
        Telescope is a tool for gathering metadata from an Airflow instance which can be processed to collect insights.
      </Text>
      <Divider marginY="5px" />
      <VStack width="60%" display="flex" alignItems="center">
        <Box width="100%" margin="0 30px" alignItems="left">
          <FormControl marginY="2%">
            <FormLabel htmlFor="telescopeOrg">
              Organization
            </FormLabel>
            <InputGroup size="sm">
              <Input
                id="telescopeOrg"
                placeholder="Astronomer, LLC"
                errorBorderColor="red.300"
                value={state.telescopeOrganizationId}
                onChange={(e) => dispatch({
                  type: 'set-telescope-org',
                  telescopeOrganizationId: e.target.value,
                })}
              />
            </InputGroup>
            <FormHelperText>
              Organization name
            </FormHelperText>
          </FormControl>
          <FormControl marginY="2%">
            <FormLabel htmlFor="telescopePresignedUrl">
              Pre-signed URL
            </FormLabel>
            <InputGroup size="sm">
              <Input
                id="telescopePresignedUrl"
                placeholder="https://storage.googleapis.com/astronomer-telescope/..."
                errorBorderColor="red.300"
                value={state.telescopePresignedUrl}
                onChange={(e) => dispatch({
                  type: 'set-telescope-presigned-url',
                  telescopePresignedUrl: e.target.value,
                })}
              />
            </InputGroup>
            <FormHelperText>
              (Optional) Enter a pre-signed URL to submit the report,
              or contact an Astronomer Representative to receive one.
            </FormHelperText>
            <FormErrorMessage>Please fill both parts.</FormErrorMessage>
          </FormControl>
          <Tooltip hasArrow label="Upload the report via pre-signed url to Astronomer">
            <Button
              leftIcon={
                isUploading ? <span /> :
                  isUploadComplete ? <FaCheck /> :
                   <GoUpload />
              }
              marginX="5px"
              isDisabled={
                isUploading || isUploadComplete || !state.telescopePresignedUrl || !state.telescopeOrganizationId
              }
              colorScheme={
                isUploadComplete ? 'green' :
                  error ? 'red' :
                    'gray'
              }
              onClick={() => {
                setIsUploading(true);
                const errFn = (err) => {
                  toast({
                      title: err.response?.data?.error || err.response?.data || err.message,
                      status: 'error',
                      isClosable: true,
                    });
                  setError(err);
                };
                axios.get(route)
                  .then((res) => {
                    console.log(res.data);
                    setIsUploadComplete(true);
                  })
                  .catch(errFn)
                  .finally(() => {
                    setIsUploading(false);
                  });
              }}
            >
              {isUploading ? <CircularProgress thickness="20px" size="30px" isIndeterminate /> :
                isUploadComplete ? '' :
                  error ? 'Error!' :
                  'Upload'
              }
            </Button>
          </Tooltip>
          <Tooltip hasArrow label="Download the report">
            <Button
              leftIcon={<GoDownload />}
              marginX="5px"
              isDisabled={!state.telescopeOrganizationId}
              href={route}
              as={Link}
              download={filename}
              target="_blank"
            >
              Download
            </Button>
          </Tooltip>
        </Box>
      </VStack>
    </Box>
  );
}
