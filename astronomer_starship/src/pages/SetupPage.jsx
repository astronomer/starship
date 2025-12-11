import React, { useEffect } from 'react';
import {
  Box,
  Button,
  Card,
  CardBody,
  Divider,
  FormControl,
  FormErrorMessage,
  FormHelperText,
  FormLabel,
  Heading,
  HStack,
  Input,
  InputGroup,
  InputLeftAddon,
  InputRightAddon,
  InputRightElement,
  Link,
  Select,
  Stack,
  Text,
  VStack,
} from '@chakra-ui/react';
import { CheckIcon, ExternalLinkIcon, RepeatIcon } from '@chakra-ui/icons';
import { IoTelescopeOutline } from 'react-icons/io5';
import { NavLink } from 'react-router-dom';
import axios from 'axios';

import { useAppState, useAppDispatch } from '../AppContext';
import ValidatedUrlCheckbox from '../component/ValidatedUrlCheckbox';
import { getHoustonRoute, getTargetUrlFromParts, proxyHeaders, proxyUrl, tokenUrlFromAirflowUrl } from '../util';
import { getWorkspaceDeploymentsQuery } from '../constants';

export default function SetupPage() {
  const state = useAppState();
  const dispatch = useAppDispatch();

  // Fetch workspace/deployment info for Software
  useEffect(() => {
    if (
      state.isSetupComplete
      && !state.isAstro
      && !(state.releaseName && state.workspaceId && state.deploymentId)
    ) {
      axios
        .post(
          proxyUrl(getHoustonRoute(state.urlOrgPart)),
          {
            operationName: 'workspaces',
            query: getWorkspaceDeploymentsQuery,
            variables: {},
          },
          { headers: proxyHeaders(state.token) },
        )
        .then((res) => {
          let found = false;
          const workspaces = res.data?.data?.workspaces || [];
          workspaces.forEach((workspace) => {
            if (found) return;
            workspace.deployments.forEach((deployment) => {
              if (found) return;
              if (deployment.releaseName === state.urlDeploymentPart) {
                dispatch({
                  type: 'set-software-info',
                  deploymentId: deployment.id,
                  releaseName: deployment.releaseName,
                  workspaceId: workspace.id,
                });
                found = true;
              }
            });
          });
        })
        .catch(() => {});
    }
  }, [state.isSetupComplete, state.isAstro, state.releaseName, state.workspaceId, state.deploymentId, state.urlOrgPart, state.urlDeploymentPart, state.token, dispatch]);

  return (
    <Box>
      <Stack
        direction={{ base: 'column', md: 'row' }}
        justify="space-between"
        align={{ base: 'flex-start', md: 'center' }}
        mb={3}
      >
        <Box>
          <Heading size="md" mb={0.5}>Getting Started</Heading>
          <Text fontSize="xs" color="gray.600">
            Configure Starship to migrate Airflow metadata between instances
          </Text>
        </Box>
        <HStack>
          <Button size="sm" leftIcon={<IoTelescopeOutline />} as={NavLink} to="/telescope" variant="outline">
            Telescope
          </Button>
          <Button
            size="sm"
            leftIcon={<RepeatIcon />}
            onClick={() => dispatch({ type: 'reset' })}
            variant="outline"
            colorScheme="red"
          >
            Reset
          </Button>
        </HStack>
      </Stack>

      <VStack spacing={3} align="stretch" w="100%">
        {/* Step 1: Product Selector */}
        <Card>
          <CardBody py={3}>
            <VStack align="stretch" spacing={2}>
              <HStack mb={1}>
                <Box
                  display="inline-flex"
                  alignItems="center"
                  justifyContent="center"
                  w={6}
                  h={6}
                  borderRadius="full"
                  bg="brand.500"
                  color="white"
                  fontWeight="bold"
                  fontSize="xs"
                  mr={2}
                >
                  1
                </Box>
                <Heading size="sm">Select Product</Heading>
              </HStack>
              <FormControl className="setup-form-field" isRequired>
                <FormLabel htmlFor="product-select" fontWeight="semibold" fontSize="sm" mb={1}>
                  Astronomer Product
                </FormLabel>
                <Select
                  id="product-select"
                  size="sm"
                  maxW="300px"
                  value={state.isAstro ? 'astro' : 'software'}
                  onChange={(e) => {
                    const newIsAstro = e.target.value === 'astro';
                    if (newIsAstro !== state.isAstro) {
                      dispatch({ type: 'toggle-is-astro' });
                    }
                  }}
                  focusBorderColor="brand.400"
                >
                  <option value="astro">Astro</option>
                  <option value="software">Astro Private Cloud (Software)</option>
                </Select>
                <FormHelperText fontSize="xs">Select the Astronomer product you are migrating to</FormHelperText>
              </FormControl>
            </VStack>
          </CardBody>
        </Card>

        {/* Step 2: URL Input */}
        <Card>
          <CardBody py={3}>
            <VStack align="stretch" spacing={2}>
              <HStack mb={1}>
                <Box
                  display="inline-flex"
                  alignItems="center"
                  justifyContent="center"
                  w={6}
                  h={6}
                  borderRadius="full"
                  bg={state.isValidUrl ? 'success.500' : 'purple.500'}
                  color="white"
                  fontWeight="bold"
                  fontSize="xs"
                  mr={2}
                >
                  {state.isValidUrl ? '✓' : '2'}
                </Box>
                <Heading size="sm">Configure Target Airflow</Heading>
              </HStack>
              <FormControl className="setup-form-field" isInvalid={state.isTouched && !state.isValidUrl} isRequired>
                <FormLabel fontWeight="semibold" fontSize="sm" mb={1}>Airflow URL</FormLabel>
                {state.isAstro ? (
                  <InputGroup size="sm">
                    <InputLeftAddon>https://</InputLeftAddon>
                    <Input
                      size="sm"
                      id="astroUrlOrgPart"
                      placeholder="claaabbbcccddd"
                      value={state.urlOrgPart}
                      isInvalid={state.isTouched && !state.isValidUrl}
                      onChange={(e) => dispatch({
                        type: 'set-url',
                        targetUrl: getTargetUrlFromParts(e.target.value, state.urlDeploymentPart, state.isAstro),
                        urlDeploymentPart: state.urlDeploymentPart,
                        urlOrgPart: e.target.value,
                      })}
                    />
                    <InputRightAddon>.astronomer.run/</InputRightAddon>
                    <Input
                      size="sm"
                      placeholder="aabbccdd"
                      value={state.urlDeploymentPart}
                      isInvalid={state.isTouched && !state.isValidUrl}
                      onChange={(e) => dispatch({
                        type: 'set-url',
                        targetUrl: getTargetUrlFromParts(state.urlOrgPart, e.target.value, state.isAstro),
                        urlOrgPart: state.urlOrgPart,
                        urlDeploymentPart: e.target.value,
                      })}
                    />
                    <InputRightAddon>/home</InputRightAddon>
                  </InputGroup>
                ) : (
                  <InputGroup size="sm">
                    <InputLeftAddon>https://deployments.</InputLeftAddon>
                    <Input
                      size="sm"
                      placeholder="basedomain.com"
                      value={state.urlOrgPart}
                      isInvalid={state.isTouched && !state.isValidUrl}
                      onChange={(e) => dispatch({
                        type: 'set-url',
                        targetUrl: getTargetUrlFromParts(e.target.value, state.urlDeploymentPart, state.isAstro),
                        urlOrgPart: e.target.value,
                        urlDeploymentPart: state.urlDeploymentPart,
                      })}
                    />
                    <InputRightAddon>/</InputRightAddon>
                    <Input
                      size="sm"
                      placeholder="release-name-1234"
                      value={state.urlDeploymentPart}
                      isInvalid={state.isTouched && !state.isValidUrl}
                      onChange={(e) => dispatch({
                        type: 'set-url',
                        targetUrl: getTargetUrlFromParts(state.urlOrgPart, e.target.value, state.isAstro),
                        urlOrgPart: state.urlOrgPart,
                        urlDeploymentPart: e.target.value,
                      })}
                    />
                    <InputRightAddon>/airflow/home</InputRightAddon>
                  </InputGroup>
                )}
                <FormHelperText fontSize="xs">
                  Enter the URL of the Airflow instance you are migrating to
                </FormHelperText>
                <FormErrorMessage>Please fill both parts of the URL</FormErrorMessage>
              </FormControl>

              <Divider my={2} />

              <FormControl isInvalid={state.isTokenTouched && !state.token} className="setup-form-field">
                <FormLabel fontWeight="semibold" fontSize="sm" mb={1}>Authentication Token</FormLabel>
                <InputGroup size="sm">
                  <Input
                    size="sm"
                    type="password"
                    value={state.token || ''}
                    isInvalid={state.isTokenTouched && !state.token}
                    placeholder="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
                    onChange={(e) => dispatch({ type: 'set-token', token: e.target.value })}
                  />
                  {state.isTokenTouched && state.token && (
                    <InputRightElement>
                      <CheckIcon color="success.500" />
                    </InputRightElement>
                  )}
                </InputGroup>
                {state.isAstro ? (
                  <FormHelperText>
                    Provide a token:{' '}
                    <Link isExternal href="https://docs.astronomer.io/astro/organization-api-tokens#create-an-organization-api-token">
                      Organization<ExternalLinkIcon mx="2px" />
                    </Link>
                    ,{' '}
                    <Link isExternal href="https://docs.astronomer.io/astro/workspace-api-tokens#create-a-workspace-api-token">
                      Workspace<ExternalLinkIcon mx="2px" />
                    </Link>
                    ,{' '}
                    <Link isExternal href={tokenUrlFromAirflowUrl(state.targetUrl)}>
                      Personal<ExternalLinkIcon mx="2px" />
                    </Link>
                    .
                  </FormHelperText>
                ) : (
                  <FormHelperText>
                    Provide a token:{' '}
                    <Link isExternal href="https://docs.astronomer.io/software/manage-workspaces#service-accounts">
                      Workspace<ExternalLinkIcon mx="2px" />
                    </Link>
                    ,{' '}
                    <Link isExternal href="https://docs.astronomer.io/software/ci-cd#step-1-create-a-service-account">
                      Deployment<ExternalLinkIcon mx="2px" />
                    </Link>
                    {state.targetUrl.startsWith('https://') && state.isValidUrl && (
                      <>
                        ,{' '}
                        <Link isExternal href={tokenUrlFromAirflowUrl(state.targetUrl)}>
                          Personal<ExternalLinkIcon mx="2px" />
                        </Link>
                      </>
                    )}
                    .
                  </FormHelperText>
                )}
                <FormErrorMessage>Please input a valid authentication token</FormErrorMessage>
              </FormControl>
            </VStack>
          </CardBody>
        </Card>

        {/* Step 3: Connection Status */}
        <Card opacity={state.targetUrl.startsWith('http') && state.isValidUrl && state.token ? 1 : 0.5}>
          <CardBody py={3}>
            <VStack align="stretch" spacing={2}>
              <HStack mb={1}>
                <Box
                  display="inline-flex"
                  alignItems="center"
                  justifyContent="center"
                  w={6}
                  h={6}
                  borderRadius="full"
                  bg={state.isAirflow && state.isStarship ? 'success.500' : 'purple.500'}
                  color="white"
                  fontWeight="bold"
                  fontSize="xs"
                  mr={2}
                >
                  {state.isAirflow && state.isStarship ? '✓' : '3'}
                </Box>
                <Heading size="sm">Connection Status</Heading>
              </HStack>
              <Box>
                <Text fontSize="xs" color="gray.600" mb={1.5}>
                  Verifying connectivity to your target Airflow instance
                </Text>
                {state.targetUrl.startsWith('http') && state.isValidUrl && state.token ? (
                  <>
                    <Box mb={1.5} p={2} bg="gray.50" borderRadius="md">
                      <Text fontSize="2xs" color="gray.500" mb={0.5}>Target URL</Text>
                      <Link isExternal href={state.targetUrl} color="brand.600" fontWeight="medium">
                        {state.targetUrl}
                        <ExternalLinkIcon mx="2px" />
                      </Link>
                    </Box>
                    <VStack align="stretch" spacing={1.5}>
                      <ValidatedUrlCheckbox
                        colorScheme="success"
                        text="Airflow API"
                        valid={state.isAirflow}
                        setValid={(value) => dispatch({ type: 'set-is-airflow', isAirflow: value })}
                        url={`${state.targetUrl}/api/v1/health`}
                        token={state.token}
                      />
                      <ValidatedUrlCheckbox
                        colorScheme="success"
                        text="Starship Plugin"
                        valid={state.isStarship}
                        setValid={(value) => dispatch({ type: 'set-is-starship', isStarship: value })}
                        url={`${state.targetUrl}/api/starship/info`}
                        token={state.token}
                      />
                    </VStack>
                  </>
                ) : (
                  <Text fontSize="sm" color="gray.500" fontStyle="italic">
                    Complete Step 2 to verify connections
                  </Text>
                )}
              </Box>
            </VStack>
          </CardBody>
        </Card>
      </VStack>
    </Box>
  );
}
