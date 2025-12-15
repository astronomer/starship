import React, { useEffect, useState } from 'react';
import {
  AlertDialog,
  AlertDialogBody,
  AlertDialogContent,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogOverlay,
  Box,
  Button,
  Card,
  CardBody,
  Collapse,
  Divider,
  FormControl,
  FormErrorMessage,
  FormHelperText,
  FormLabel,
  Heading,
  HStack,
  Icon,
  IconButton,
  Input,
  InputGroup,
  InputLeftAddon,
  InputRightAddon,
  InputRightElement,
  Link,
  Select,
  Stack,
  Text,
  Tooltip,
  useDisclosure,
  VStack,
} from '@chakra-ui/react';
import {
  CheckIcon,
  ChevronDownIcon,
  ChevronUpIcon,
  ExternalLinkIcon,
  InfoIcon,
  QuestionIcon,
  RepeatIcon,
} from '@chakra-ui/icons';
import { IoTelescopeOutline } from 'react-icons/io5';
import { NavLink } from 'react-router-dom';
import axios from 'axios';

import { useAppState, useAppDispatch } from '../AppContext';
import ValidatedUrlCheckbox from '../component/ValidatedUrlCheckbox';
import {
  getHoustonRoute,
  getTargetUrlFromParts,
  proxyHeaders,
  proxyUrl,
  tokenUrlFromAirflowUrl,
} from '../util';
import { getWorkspaceDeploymentsQuery } from '../constants';

export default function SetupPage() {
  const state = useAppState();
  const dispatch = useAppDispatch();

  // Collapsible section states
  const step1 = useDisclosure({ defaultIsOpen: true });
  const step2 = useDisclosure({ defaultIsOpen: false });
  const step3 = useDisclosure({ defaultIsOpen: false });

  // Reset confirmation dialog
  const resetDialog = useDisclosure();
  const cancelRef = React.useRef();

  // Animation states for checkmarks
  const [showStep1Check, setShowStep1Check] = useState(false);
  const [showStep2Check, setShowStep2Check] = useState(false);
  const [showStep3Check, setShowStep3Check] = useState(false);

  // Step completion logic
  const isStep1Complete = state.isAstro !== undefined;
  const isStep2Complete = state.isValidUrl && state.token;
  const isStep3Complete = state.isAirflow && state.isStarship;

  // Auto-open sections on mount based on completion state
  useEffect(() => {
    if (isStep1Complete && !isStep2Complete && !step2.isOpen) {
      step2.onOpen();
    }
    if (isStep2Complete && !isStep3Complete && !step3.isOpen) {
      step3.onOpen();
    }
  }, [isStep1Complete, isStep2Complete, isStep3Complete, step2.isOpen, step3.isOpen]);

  // Trigger animations when steps complete
  useEffect(() => {
    if (isStep1Complete && !showStep1Check) {
      setShowStep1Check(true);
      // Open next section when step 1 is complete
      if (!step2.isOpen) {
        setTimeout(() => step2.onOpen(), 300);
      }
    }
  }, [isStep1Complete, showStep1Check, step2]);

  useEffect(() => {
    if (isStep2Complete && !showStep2Check) {
      setShowStep2Check(true);
      setTimeout(() => {
        step2.onClose();
        // Open next section when step 2 is complete
        step3.onOpen();
      }, 1000);
    }
  }, [isStep2Complete, showStep2Check, step2, step3]);

  useEffect(() => {
    if (isStep3Complete && !showStep3Check) {
      setShowStep3Check(true);
      setTimeout(() => step3.onClose(), 1000);
    }
  }, [isStep3Complete, showStep3Check, step3]);

  // Fetch workspace/deployment info for Software
  useEffect(() => {
    if (state.isSetupComplete
      && !state.isAstro
      && !(state.releaseName && state.workspaceId && state.deploymentId)) {
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
  }, [
    state.isSetupComplete,
    state.isAstro,
    state.releaseName,
    state.workspaceId,
    state.deploymentId,
    state.urlOrgPart,
    state.urlDeploymentPart,
    state.token,
    dispatch,
  ]);

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
            onClick={resetDialog.onOpen}
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
                  bg={isStep1Complete ? 'success.500' : 'brand.500'}
                  color="white"
                  fontWeight="bold"
                  fontSize="xs"
                  mr={2}
                  transition="all 0.3s"
                >
                  {isStep1Complete && showStep1Check ? '✓' : '1'}
                </Box>
                <Heading size="sm">Select Product</Heading>
                <Tooltip
                  label="Choose whether you're migrating to Astro (cloud) or Astro Private Cloud (self-hosted)"
                  placement="top"
                  hasArrow
                >
                  <QuestionIcon color="gray.400" boxSize={3} cursor="help" />
                </Tooltip>
              </HStack>
              <Collapse in={step1.isOpen} animateOpacity>
                <FormControl className="setup-form-field" isRequired>
                  <FormLabel htmlFor="product-select" mb={1}>
                    Astronomer Product
                  </FormLabel>
                  <Select
                    id="product-select"
                    size="sm"
                    maxW="xs"
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
                  <FormHelperText fontSize="xs">
                    Select the Astronomer product you are migrating to
                  </FormHelperText>
                </FormControl>
              </Collapse>
            </VStack>
          </CardBody>
        </Card>

        {/* Step 2: URL Input */}
        <Card
          opacity={isStep1Complete ? 1 : 0.5}
          pointerEvents={isStep1Complete ? 'auto' : 'none'}
          transition="all 0.3s"
        >
          <CardBody py={3}>
            <VStack align="stretch" spacing={2}>
              <HStack
                mb={1}
                justify="space-between"
                cursor={isStep2Complete ? 'pointer' : 'default'}
                onClick={isStep2Complete ? step2.onToggle : undefined}
                _hover={isStep2Complete ? { bg: 'gray.50' } : undefined}
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
                    bg={isStep2Complete ? 'success.500' : 'purple.500'}
                    color="white"
                    fontWeight="bold"
                    fontSize="xs"
                    mr={2}
                    transition="all 0.3s"
                  >
                    {isStep2Complete && showStep2Check ? '✓' : '2'}
                  </Box>
                  <Heading size="sm">Configure Target Airflow</Heading>
                  <Tooltip
                    label="Enter the URL and authentication token for your target Airflow deployment"
                    placement="top"
                    hasArrow
                  >
                    <QuestionIcon color="gray.400" boxSize={3} cursor="help" />
                  </Tooltip>
                </HStack>
                {isStep2Complete && (
                  <ChevronDownIcon
                    boxSize={4}
                    transform={step2.isOpen ? 'rotate(180deg)' : 'rotate(0deg)'}
                    transition="transform 0.2s"
                  />
                )}
              </HStack>
              <Collapse in={step2.isOpen} animateOpacity>
                <FormControl
                  className="setup-form-field"
                  isInvalid={state.isTouched && !state.isValidUrl}
                  isRequired
                >
                  <HStack mb={1}>
                    <FormLabel mb={0}>Airflow URL</FormLabel>
                    <Tooltip
                      label={
                        state.isAstro
                          ? 'Find your deployment URL in the Astro Cloud UI under Deployment Settings'
                          : 'Find your deployment URL in the Software UI or from your platform team'
                      }
                      placement="top"
                      hasArrow
                    >
                      <InfoIcon color="gray.400" boxSize={3} cursor="help" />
                    </Tooltip>
                  </HStack>
                  {state.isAstro ? (
                    <Stack direction={{ base: 'column', lg: 'row' }} spacing={2}>
                      <InputGroup size="sm">
                        <InputLeftAddon>https://</InputLeftAddon>
                        <Input
                          id="astroUrlOrgPart"
                          placeholder="claaabbbcccddd"
                          value={state.urlOrgPart}
                          isInvalid={state.isTouched && !state.isValidUrl}
                          onChange={(e) => dispatch({
                            type: 'set-url',
                            targetUrl: getTargetUrlFromParts(
                              e.target.value,
                              state.urlDeploymentPart,
                              state.isAstro,
                            ),
                            urlDeploymentPart: state.urlDeploymentPart,
                            urlOrgPart: e.target.value,
                          })}
                        />
                        <InputRightAddon>.astronomer.run/</InputRightAddon>
                      </InputGroup>
                      <InputGroup size="sm">
                        <Input
                          placeholder="aabbccdd"
                          value={state.urlDeploymentPart}
                          isInvalid={state.isTouched && !state.isValidUrl}
                          onChange={(e) => dispatch({
                            type: 'set-url',
                            targetUrl: getTargetUrlFromParts(
                              state.urlOrgPart,
                              e.target.value,
                              state.isAstro,
                            ),
                            urlOrgPart: state.urlOrgPart,
                            urlDeploymentPart: e.target.value,
                          })}
                        />
                        <InputRightAddon>/home</InputRightAddon>
                      </InputGroup>
                    </Stack>
                  ) : (
                    <Stack direction={{ base: 'column', lg: 'row' }} spacing={2}>
                      <InputGroup size="sm">
                        <InputLeftAddon>https://deployments.</InputLeftAddon>
                        <Input
                          placeholder="basedomain.com"
                          value={state.urlOrgPart}
                          isInvalid={state.isTouched && !state.isValidUrl}
                          onChange={(e) => dispatch({
                            type: 'set-url',
                            targetUrl: getTargetUrlFromParts(
                              e.target.value,
                              state.urlDeploymentPart,
                              state.isAstro,
                            ),
                            urlOrgPart: e.target.value,
                            urlDeploymentPart: state.urlDeploymentPart,
                          })}
                        />
                        <InputRightAddon>/</InputRightAddon>
                      </InputGroup>
                      <InputGroup size="sm">
                        <Input
                          placeholder="release-name-1234"
                          value={state.urlDeploymentPart}
                          isInvalid={state.isTouched && !state.isValidUrl}
                          onChange={(e) => dispatch({
                            type: 'set-url',
                            targetUrl: getTargetUrlFromParts(
                              state.urlOrgPart,
                              e.target.value,
                              state.isAstro,
                            ),
                            urlOrgPart: state.urlOrgPart,
                            urlDeploymentPart: e.target.value,
                          })}
                        />
                        <InputRightAddon>/airflow/home</InputRightAddon>
                      </InputGroup>
                    </Stack>
                  )}
                  <FormHelperText fontSize="xs">
                    Enter the URL of the Airflow instance you are migrating to
                  </FormHelperText>
                  <FormErrorMessage>Please fill both parts of the URL</FormErrorMessage>
                </FormControl>

                <Divider my={2} />

                <FormControl
                  isInvalid={state.isTokenTouched && !state.token}
                  className="setup-form-field"
                >
                  <HStack mb={1}>
                    <FormLabel mb={0}>Authentication Token</FormLabel>
                    <Tooltip
                      label={
                        state.isAstro
                          ? 'Use an Organization, Workspace, or Personal access token with read/write permissions'
                          : 'Use a Workspace or Deployment service account token with appropriate permissions'
                      }
                      placement="top"
                      hasArrow
                    >
                      <InfoIcon color="gray.400" boxSize={3} cursor="help" />
                    </Tooltip>
                  </HStack>
                  <InputGroup size="sm">
                    <Input
                      type="password"
                      autoComplete="off"
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
                      <Tooltip
                        label="Organization tokens have access to all workspaces and deployments"
                        hasArrow
                      >
                        <Link
                          isExternal
                          href="https://docs.astronomer.io/astro/organization-api-tokens#create-an-organization-api-token"
                        >
                          Organization<ExternalLinkIcon mx="2px" />
                        </Link>
                      </Tooltip>
                      ,{' '}
                      <Tooltip
                        label="Workspace tokens have access to all deployments in a workspace"
                        hasArrow
                      >
                        <Link
                          isExternal
                          href="https://docs.astronomer.io/astro/workspace-api-tokens#create-a-workspace-api-token"
                        >
                          Workspace<ExternalLinkIcon mx="2px" />
                        </Link>
                      </Tooltip>
                      ,{' '}
                      <Tooltip label="Personal tokens are user-specific access tokens" hasArrow>
                        <Link isExternal href={tokenUrlFromAirflowUrl(state.targetUrl)}>
                          Personal<ExternalLinkIcon mx="2px" />
                        </Link>
                      </Tooltip>
                      .
                    </FormHelperText>
                  ) : (
                    <FormHelperText>
                      Provide a token:{' '}
                      <Tooltip
                        label="Workspace service accounts have access to all deployments in a workspace"
                        hasArrow
                      >
                        <Link
                          isExternal
                          href="https://docs.astronomer.io/software/manage-workspaces#service-accounts"
                        >
                          Workspace<ExternalLinkIcon mx="2px" />
                        </Link>
                      </Tooltip>
                      ,{' '}
                      <Tooltip
                        label="Deployment service accounts are deployment-specific tokens"
                        hasArrow
                      >
                        <Link
                          isExternal
                          href="https://docs.astronomer.io/software/ci-cd#step-1-create-a-service-account"
                        >
                          Deployment<ExternalLinkIcon mx="2px" />
                        </Link>
                      </Tooltip>
                      {state.targetUrl.startsWith('https://') && state.isValidUrl && (
                        <>
                          ,{' '}
                          <Tooltip
                            label="Personal tokens are user-specific access tokens"
                            hasArrow
                          >
                            <Link isExternal href={tokenUrlFromAirflowUrl(state.targetUrl)}>
                              Personal<ExternalLinkIcon mx="2px" />
                            </Link>
                          </Tooltip>
                        </>
                      )}
                      .
                    </FormHelperText>
                  )}
                  <FormErrorMessage>
                    Please input a valid authentication token
                  </FormErrorMessage>
                </FormControl>
              </Collapse>
            </VStack>
          </CardBody>
        </Card>

        {/* Step 3: Connection Status */}
        <Card
          opacity={isStep2Complete ? 1 : 0.5}
          pointerEvents={isStep2Complete ? 'auto' : 'none'}
          transition="all 0.3s"
        >
          <CardBody py={3}>
            <VStack align="stretch" spacing={2}>
              <HStack
                mb={1}
                justify="space-between"
                cursor={isStep3Complete ? 'pointer' : 'default'}
                onClick={isStep3Complete ? step3.onToggle : undefined}
                _hover={isStep3Complete ? { bg: 'gray.50' } : undefined}
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
                    bg={isStep3Complete ? 'success.500' : 'purple.500'}
                    color="white"
                    fontWeight="bold"
                    fontSize="xs"
                    mr={2}
                    transition="all 0.3s"
                  >
                    {isStep3Complete && showStep3Check ? '✓' : '3'}
                  </Box>
                  <Heading size="sm">Connection Status</Heading>
                  <Tooltip
                    label="Verifies that both Airflow API and Starship plugin are accessible"
                    placement="top"
                    hasArrow
                  >
                    <QuestionIcon color="gray.400" boxSize={3} cursor="help" />
                  </Tooltip>
                </HStack>
                {isStep3Complete && (
                  <ChevronDownIcon
                    boxSize={4}
                    transform={step3.isOpen ? 'rotate(180deg)' : 'rotate(0deg)'}
                    transition="transform 0.2s"
                  />
                )}
              </HStack>
              <Collapse in={step3.isOpen} animateOpacity>
                <Box>
                  <Text fontSize="xs" color="gray.600" mb={1.5}>
                    Verifying connectivity to your target Airflow instance
                  </Text>
                  {state.targetUrl.startsWith('http') && state.isValidUrl && state.token ? (
                    <>
                      <Box mb={1.5} p={2} bg="gray.50" borderRadius="md">
                        <Text fontSize="2xs" color="gray.500" mb={0.5}>
                          Target URL
                        </Text>
                        <Link
                          isExternal
                          href={state.targetUrl}
                          color="brand.600"
                          fontWeight="medium"
                          wordBreak="break-all"
                          overflowWrap="anywhere"
                          display="inline"
                        >
                          {state.targetUrl}
                          <ExternalLinkIcon mx="2px" verticalAlign="middle" />
                        </Link>
                      </Box>
                      <VStack align="stretch" spacing={1.5}>
                        <ValidatedUrlCheckbox
                          colorScheme="success"
                          text="Airflow API"
                          valid={state.isAirflow}
                          setValid={(value) => dispatch({
                            type: 'set-is-airflow',
                            isAirflow: value,
                          })}
                          url={`${state.targetUrl}/api/v1/health`}
                          token={state.token}
                        />
                        <ValidatedUrlCheckbox
                          colorScheme="success"
                          text="Starship Plugin"
                          valid={state.isStarship}
                          setValid={(value) => dispatch({
                            type: 'set-is-starship',
                            isStarship: value,
                          })}
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
              </Collapse>
            </VStack>
          </CardBody>
        </Card>

        {/* Success Message - Show when all steps complete */}
        {isStep3Complete && (
          <Card bg="success.50" borderColor="success.500" borderWidth="1px">
            <CardBody py={4}>
              <VStack align="stretch" spacing={3}>
                <HStack>
                  <CheckIcon color="success.500" boxSize={5} />
                  <Heading size="sm" color="success.700">
                    Setup Complete!
                  </Heading>
                </HStack>
                <Text fontSize="sm" color="gray.700">
                  Your Starship configuration is ready. You can now migrate your Airflow metadata by using the navigation links at the top of the page.
                </Text>
              </VStack>
            </CardBody>
          </Card>
        )}
      </VStack>

      {/* Reset Confirmation Dialog */}
      <AlertDialog
        isOpen={resetDialog.isOpen}
        leastDestructiveRef={cancelRef}
        onClose={resetDialog.onClose}
      >
        <AlertDialogOverlay>
          <AlertDialogContent>
            <AlertDialogHeader fontSize="lg" fontWeight="bold">
              Reset Configuration
            </AlertDialogHeader>

            <AlertDialogBody>
              Are you sure you want to reset all configuration? This will clear your
              deployment URL, authentication token, and connection status.
            </AlertDialogBody>

            <AlertDialogFooter>
              <Button ref={cancelRef} onClick={resetDialog.onClose} size="sm" variant="outline">
                Cancel
              </Button>
              <Button
                colorScheme="red"
                onClick={() => {
                  dispatch({ type: 'reset' });
                  resetDialog.onClose();
                  setShowStep1Check(false);
                  setShowStep2Check(false);
                  setShowStep3Check(false);
                  step1.onOpen();
                  step2.onOpen();
                  step3.onOpen();
                }}
                ml={3}
                size="sm"
              >
                Reset
              </Button>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialogOverlay>
      </AlertDialog>
    </Box>
  );
}
