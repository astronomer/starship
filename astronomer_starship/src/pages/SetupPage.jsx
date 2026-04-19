import React, { useCallback, useEffect, useState, useRef } from 'react';
import {
  AlertDialog,
  AlertDialogBody,
  AlertDialogContent,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogOverlay,
  Box,
  Button,
  Heading,
  HStack,
  Stack,
  Text,
  useDisclosure,
  useToast,
  VStack,
} from '@chakra-ui/react';
import { RepeatIcon } from '@chakra-ui/icons';
import { IoTelescopeOutline } from 'react-icons/io5';
import { NavLink } from 'react-router-dom';
import axios from 'axios';
import { useAppState, useAppDispatch } from '../AppContext';
import SetupStep from '../component/SetupStep';
import UrlTokenForm from '../component/UrlTokenForm';
import ConnectionStatus from '../component/ConnectionStatus';
import SourcePlatformPicker from '../component/SourcePlatformPicker';
import SourceCredentialsForm from '../component/SourceCredentialsForm';
import SourceConnectionStatus from '../component/SourceConnectionStatus';
import { getHoustonRoute, proxyHeaders, proxyUrl } from '../util';
import { getWorkspaceDeploymentsQuery } from '../constants';

export default function SetupPage() {
  const state = useAppState();
  const dispatch = useAppDispatch();
  const toast = useToast();

  // Collapsible section states
  const step1 = useDisclosure({ defaultIsOpen: true });
  const step2 = useDisclosure({ defaultIsOpen: false });
  // Source steps are collapsed by default — only matter for Cutover users.
  const step3 = useDisclosure({ defaultIsOpen: false });
  const step4 = useDisclosure({ defaultIsOpen: false });

  // Reset confirmation dialog
  const resetDialog = useDisclosure();
  const cancelRef = React.useRef();

  // Track if setup complete toast has been shown
  const hasShownCompleteToast = useRef(false);

  // Animation states for checkmarks
  const [showStep1Check, setShowStep1Check] = useState(false);
  const [showStep2Check, setShowStep2Check] = useState(false);
  const [showStep3Check, setShowStep3Check] = useState(false);
  const [showStep4Check, setShowStep4Check] = useState(false);

  // Step completion logic
  const isStep1Complete = state.isValidUrl && state.token;
  const isStep2Complete = state.isAirflow && state.isStarship;

  // Source creds present for the selected platform
  const sourceHasCreds = (() => {
    switch (state.sourcePlatform) {
      case 'astro':
        return !!state.sourceToken;
      case 'oss':
        return !!state.sourceToken || !!(state.sourceLogin && state.sourcePassword);
      case 'gcc':
        return true;
      case 'mwaa':
        return !!state.sourceRegion;
      default:
        return false;
    }
  })();
  const isStep3Complete = !!state.sourcePlatform && state.sourceIsValidUrl && sourceHasCreds;
  const isStep4Complete = state.isSourceSetupComplete;

  // Auto-open sections on mount based on completion state
  useEffect(() => {
    if (isStep1Complete && !isStep2Complete && !step2.isOpen) {
      step2.onOpen();
    }
  }, [isStep1Complete, isStep2Complete, step2]);

  // Trigger animations when steps complete
  useEffect(() => {
    if (isStep1Complete && !showStep1Check) {
      setShowStep1Check(true);
      setTimeout(() => {
        step1.onClose();
        step2.onOpen();
      }, 1000);
    }
  }, [isStep1Complete, showStep1Check, step1, step2]);

  useEffect(() => {
    if (isStep2Complete && !showStep2Check) {
      setShowStep2Check(true);
      setTimeout(() => step2.onClose(), 1000);
    }
  }, [isStep2Complete, showStep2Check, step2]);

  // Auto-open source connection-status step once creds + URL are in place.
  useEffect(() => {
    if (isStep3Complete && !isStep4Complete && !step4.isOpen) {
      step4.onOpen();
    }
  }, [isStep3Complete, isStep4Complete, step4]);

  useEffect(() => {
    if (isStep3Complete && !showStep3Check) {
      setShowStep3Check(true);
    }
  }, [isStep3Complete, showStep3Check]);

  useEffect(() => {
    if (isStep4Complete && !showStep4Check) {
      setShowStep4Check(true);
      setTimeout(() => step4.onClose(), 1000);
    }
  }, [isStep4Complete, showStep4Check, step4]);

  // Show success toast when setup completes
  useEffect(() => {
    if (isStep2Complete && !hasShownCompleteToast.current) {
      hasShownCompleteToast.current = true;
      toast({
        title: 'Setup Complete!',
        description:
          'Your Starship configuration is ready. Use the navigation links above to migrate your Airflow metadata.',
        status: 'success',
        duration: 6000,
        isClosable: true,
        variant: 'outline',
      });
    }
  }, [isStep2Complete, toast]);

  // Fetch workspace/deployment info for Software
  useEffect(() => {
    if (state.isSetupComplete && !state.isAstro && !(state.releaseName && state.workspaceId && state.deploymentId)) {
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

  const handleReset = () => {
    dispatch({ type: 'reset' });
    resetDialog.onClose();
    setShowStep1Check(false);
    setShowStep2Check(false);
    setShowStep3Check(false);
    setShowStep4Check(false);
    hasShownCompleteToast.current = false;
    step1.onOpen();
    step2.onOpen();
    step3.onClose();
    step4.onClose();
  };

  // Memoize callbacks to prevent infinite re-renders in ValidatedUrlCheckbox
  const handleAirflowChange = useCallback(
    (value) => dispatch({ type: 'set-is-airflow', isAirflow: value }),
    [dispatch],
  );
  const handleStarshipChange = useCallback(
    (value) => dispatch({ type: 'set-is-starship', isStarship: value }),
    [dispatch],
  );

  // Source-side callbacks
  const handleSourcePlatformChange = useCallback(
    (platform) => dispatch({ type: 'set-source-platform', platform }),
    [dispatch],
  );
  const handleSourceUrlChange = useCallback(
    (sourceUrl) => dispatch({ type: 'set-source-url', sourceUrl }),
    [dispatch],
  );
  const handleSourceCredsChange = useCallback(
    (partial) => dispatch({ type: 'set-source-creds', ...partial }),
    [dispatch],
  );
  const handleSourceAirflowChange = useCallback(
    (value) => dispatch({ type: 'set-source-is-airflow', isAirflow: value }),
    [dispatch],
  );
  const handleSourceStarshipChange = useCallback(
    (value) => dispatch({ type: 'set-source-is-starship', isStarship: value }),
    [dispatch],
  );
  const handleSourceConnectionSavedChange = useCallback(
    (saved) => dispatch({ type: 'set-source-connection-saved', saved }),
    [dispatch],
  );

  return (
    <Box>
      <Stack
        direction={{ base: 'column', md: 'row' }}
        justify="space-between"
        align={{ base: 'flex-start', md: 'center' }}
        mb={3}
      >
        <Box>
          <Heading size="md" mb={0.5}>
            Getting Started
          </Heading>
          <Text fontSize="xs" color="gray.600">
            Configure Starship to migrate Airflow metadata between instances
          </Text>
        </Box>
        <HStack>
          <Button size="sm" leftIcon={<IoTelescopeOutline />} as={NavLink} to="/telescope" variant="outline">
            Telescope
          </Button>
          <Button size="sm" leftIcon={<RepeatIcon />} onClick={resetDialog.onOpen} variant="outline" colorScheme="red">
            Reset
          </Button>
        </HStack>
      </Stack>

      <VStack spacing={3} align="stretch" w="100%">
        {/* Step 1: URL & Token Input */}
        <SetupStep
          stepNumber="1"
          title="Configure Target Airflow"
          tooltip="Enter the URL and authentication token for your target deployment"
          isComplete={isStep1Complete}
          showCheck={showStep1Check}
          isOpen={step1.isOpen}
          onToggle={step1.onToggle}
        >
          <UrlTokenForm
            targetUrl={state.targetUrl}
            token={state.token}
            isAstro={state.isAstro}
            isTouched={state.isTouched}
            isTokenTouched={state.isTokenTouched}
            isValidUrl={state.isValidUrl}
            onUrlChange={(urlData) => dispatch({ type: 'set-url', ...urlData })}
            onTokenChange={(token) => dispatch({ type: 'set-token', token })}
            onToggleIsAstro={() => dispatch({ type: 'toggle-is-astro' })}
          />
        </SetupStep>

        {/* Step 2: Connection Status */}
        <SetupStep
          stepNumber="2"
          title="Connection Status"
          tooltip="Verifies that both Airflow API and Starship plugin are accessible"
          isComplete={isStep2Complete}
          showCheck={showStep2Check}
          isOpen={step2.isOpen}
          onToggle={step2.onToggle}
          isDisabled={!isStep1Complete}
        >
          <ConnectionStatus
            targetUrl={state.targetUrl}
            token={state.token}
            isValidUrl={state.isValidUrl}
            isAirflow={state.isAirflow}
            isStarship={state.isStarship}
            onAirflowChange={handleAirflowChange}
            onStarshipChange={handleStarshipChange}
          />
        </SetupStep>

        {/* Step 3: Source Airflow (Cutover) — optional */}
        <SetupStep
          stepNumber="3"
          title="Configure Source Airflow (optional — enables Cutover)"
          tooltip="Required only if you're migrating DAG metadata INTO this Airflow. Saved as an Airflow Connection named `starship_source`."
          isComplete={isStep3Complete}
          showCheck={showStep3Check}
          isOpen={step3.isOpen}
          onToggle={step3.onToggle}
        >
          <VStack align="stretch" spacing={4}>
            <Text fontSize="xs" color="gray.600">
              Pick the platform of the Airflow you are migrating FROM, then provide its URL and credentials. When
              the connection is verified and saved, the <strong>Cutover</strong> tabs unlock in the navigation above.
            </Text>
            <SourcePlatformPicker value={state.sourcePlatform} onChange={handleSourcePlatformChange} />
            {state.sourcePlatform && (
              <SourceCredentialsForm
                platform={state.sourcePlatform}
                url={state.sourceUrl}
                token={state.sourceToken}
                login={state.sourceLogin}
                password={state.sourcePassword}
                impersonationChain={state.sourceImpersonationChain}
                region={state.sourceRegion}
                roleArn={state.sourceRoleArn}
                environmentName={state.sourceEnvironmentName}
                isTouched={state.sourceIsTouched}
                credsTouched={state.sourceCredsTouched}
                isValidUrl={state.sourceIsValidUrl}
                onUrlChange={handleSourceUrlChange}
                onCredsChange={handleSourceCredsChange}
              />
            )}
          </VStack>
        </SetupStep>

        {/* Step 4: Source Connection Status — save as Airflow Connection */}
        <SetupStep
          stepNumber="4"
          title="Source Connection Status"
          tooltip="Verifies source connectivity and saves the Airflow Connection `starship_source` used by the Cutover Tool."
          isComplete={isStep4Complete}
          showCheck={showStep4Check}
          isOpen={step4.isOpen}
          onToggle={step4.onToggle}
          isDisabled={!isStep3Complete}
        >
          <SourceConnectionStatus
            platform={state.sourcePlatform}
            url={state.sourceUrl}
            token={state.sourceToken}
            login={state.sourceLogin}
            password={state.sourcePassword}
            impersonationChain={state.sourceImpersonationChain}
            region={state.sourceRegion}
            roleArn={state.sourceRoleArn}
            environmentName={state.sourceEnvironmentName}
            isValidUrl={state.sourceIsValidUrl}
            isAirflow={state.sourceIsAirflow}
            isStarship={state.sourceIsStarship}
            connectionSaved={state.sourceConnectionSaved}
            hasCreds={sourceHasCreds}
            onAirflowChange={handleSourceAirflowChange}
            onStarshipChange={handleSourceStarshipChange}
            onConnectionSavedChange={handleSourceConnectionSavedChange}
          />
        </SetupStep>
      </VStack>

      {/* Reset Confirmation Dialog */}
      <AlertDialog isOpen={resetDialog.isOpen} leastDestructiveRef={cancelRef} onClose={resetDialog.onClose}>
        <AlertDialogOverlay>
          <AlertDialogContent>
            <AlertDialogHeader fontSize="lg" fontWeight="bold">
              Reset Configuration
            </AlertDialogHeader>

            <AlertDialogBody>
              Are you sure you want to reset all configuration? This will clear your deployment URL, authentication
              token, and connection status.
            </AlertDialogBody>

            <AlertDialogFooter>
              <Button ref={cancelRef} onClick={resetDialog.onClose} size="sm" variant="outline">
                Cancel
              </Button>
              <Button colorScheme="red" onClick={handleReset} ml={3} size="sm">
                Reset
              </Button>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialogOverlay>
      </AlertDialog>
    </Box>
  );
}
