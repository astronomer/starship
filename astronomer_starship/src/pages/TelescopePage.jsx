import {
  Box, Button,
  Divider, FormControl,
  FormErrorMessage,
  FormHelperText,
  FormLabel,
  Input, InputGroup,
  Text, Tooltip, VStack,
} from '@chakra-ui/react';
import React from 'react';
import { GoDownload, GoUpload } from 'react-icons/go';

export default function TelescopePage({ state, dispatch }) {
  return (
    <Box>
      <Text fontSize="xl">
        Telescope is a tool for gathering metadata
      </Text>
      <Divider marginY="5px" />
      <VStack width="60%" display="flex" alignItems="center">
        <Box width="100%" margin="0 30px" alignItems="left">
          <FormControl marginY="2%">
            <FormLabel htmlFor="telescopeOrg">
              Organization
            </FormLabel>
            <InputGroup size="sm">
              {/* <InputLeftAddon>https://</InputLeftAddon> */}
              <Input
                id="telescopeOrg"
                placeholder="Company XYZ"
                errorBorderColor="red.300"
                value={state.telescopeOrg}
                onChange={(e) => dispatch({
                  type: 'set-telescope-org',
                  telescopeOrg: e.target.value,
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
              {/* <InputLeftAddon>https://</InputLeftAddon> */}
              <Input
                id="telescopePresignedUrl"
                placeholder="https://storage.googleapis.com/astronomer-telescope..."
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
              leftIcon={<GoUpload />}
              marginX="5px"
              isDisabled={!state.telescopePresignedUrl}
              onClick={() => console.log('upload')}
            >
              Upload
            </Button>
          </Tooltip>
          <Tooltip hasArrow label="Download the report">
            <Button
              leftIcon={<GoDownload />}
              marginX="5px"
              onClick={() => console.log('download')}
            >
              Download
            </Button>
          </Tooltip>
        </Box>
      </VStack>
    </Box>
  );
}
