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
import { useAppState, useAppDispatch } from '../AppContext';
import SetupStep from '../component/SetupStep';
import SourcePlatformPicker from '../component/SourcePlatformPicker';
import SourceCredentialsForm from '../component/SourceCredentialsForm';
import SourceConnectionStatus from '../component/SourceConnectionStatus';

/**
 * Source Setup — configures the Airflow instance that this deployment will
 * migrate DAG metadata FROM. Required only for the Cutover Tool; unrelated
 * to the classic Starship push flow configured in Target Setup.
 */
export default function SourceSetupPage() {
  const state = useAppState();
  const dispatch = useAppDispatch();
  const toast = useToast();

  // Step 1 is open on mount so first-time users aren't stuck with a closed step.
  const step1 = useDisclosure({ defaultIsOpen: true });
  const step2 = useDisclosure({ defaultIsOpen: false });

  const resetDialog = useDisclosure();
  const cancelRef = React.useRef();

  const hasShownCompleteToast = useRef(false);

  const [showStep1Check, setShowStep1Check] = useState(false);
  const [showStep2Check, setShowStep2Check] = useState(false);

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

  const isStep1Complete = !!state.sourcePlatform && state.sourceIsValidUrl && sourceHasCreds;
  const isStep2Complete = state.isSourceSetupComplete;

  // Auto-open Step 2 once creds are in place.
  useEffect(() => {
    if (isStep1Complete && !isStep2Complete && !step2.isOpen) {
      step2.onOpen();
    }
  }, [isStep1Complete, isStep2Complete, step2]);

  // Collapse Step 1 with a brief check animation once it's complete.
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

  useEffect(() => {
    if (isStep2Complete && !hasShownCompleteToast.current) {
      hasShownCompleteToast.current = true;
      toast({
        title: 'Source Setup Complete!',
        description: 'The Cutover tab is now enabled in the navigation above.',
        status: 'success',
        duration: 6000,
        isClosable: true,
        variant: 'outline',
      });
    }
  }, [isStep2Complete, toast]);

  const handleReset = () => {
    dispatch({ type: 'reset-source' });
    resetDialog.onClose();
    setShowStep1Check(false);
    setShowStep2Check(false);
    hasShownCompleteToast.current = false;
    step1.onOpen();
    step2.onClose();
  };

  const handleSourcePlatformChange = useCallback(
    (platform) => dispatch({ type: 'set-source-platform', platform }),
    [dispatch],
  );
  const handleSourceConnIdChange = useCallback(
    (connId) => dispatch({ type: 'set-source-conn-id', connId }),
    [dispatch],
  );
  const handleSourceUrlChange = useCallback((sourceUrl) => dispatch({ type: 'set-source-url', sourceUrl }), [dispatch]);
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
            Source Setup
          </Heading>
          <Text fontSize="xs" color="gray.600">
            Configure the Airflow instance you want to migrate metadata <strong>from</strong>. Required to enable the
            Cutover Tool.
          </Text>
        </Box>
        <HStack>
          <Button size="sm" leftIcon={<RepeatIcon />} onClick={resetDialog.onOpen} variant="outline" colorScheme="red">
            Reset
          </Button>
        </HStack>
      </Stack>

      <VStack spacing={3} align="stretch" w="100%">
        <SetupStep
          stepNumber="1"
          title="Configure Source Airflow"
          tooltip="Pick the source platform and provide its URL and credentials."
          isComplete={isStep1Complete}
          showCheck={showStep1Check}
          isOpen={step1.isOpen}
          onToggle={step1.onToggle}
        >
          <VStack align="stretch" spacing={4}>
            <Text fontSize="xs" color="gray.600">
              Pick the platform of the Airflow you are migrating FROM, then provide its URL and credentials. When the
              connection is verified and saved, the <strong>Cutover</strong> tab unlocks in the navigation above.
            </Text>
            <SourcePlatformPicker value={state.sourcePlatform} onChange={handleSourcePlatformChange} />
            {state.sourcePlatform && (
              <SourceCredentialsForm
                platform={state.sourcePlatform}
                connId={state.sourceConnId}
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
                onConnIdChange={handleSourceConnIdChange}
                onUrlChange={handleSourceUrlChange}
                onCredsChange={handleSourceCredsChange}
              />
            )}
          </VStack>
        </SetupStep>

        <SetupStep
          stepNumber="2"
          title="Source Connection Status"
          tooltip="Verifies source connectivity and saves the Airflow Connection `starship_source` used by the Cutover Tool."
          isComplete={isStep2Complete}
          showCheck={showStep2Check}
          isOpen={step2.isOpen}
          onToggle={step2.onToggle}
          isDisabled={!isStep1Complete}
        >
          <SourceConnectionStatus
            platform={state.sourcePlatform}
            connId={state.sourceConnId}
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

      <AlertDialog isOpen={resetDialog.isOpen} leastDestructiveRef={cancelRef} onClose={resetDialog.onClose}>
        <AlertDialogOverlay>
          <AlertDialogContent>
            <AlertDialogHeader fontSize="lg" fontWeight="bold">
              Reset Source Configuration
            </AlertDialogHeader>

            <AlertDialogBody>
              Are you sure you want to reset the source configuration? This clears the platform, URL, and credentials
              you provided here. The Airflow Connection <code>starship_source</code> will not be deleted from this
              Airflow. Target Setup will remain untouched.
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
