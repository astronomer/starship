import PropTypes from 'prop-types';
import {
  Alert,
  AlertIcon,
  Divider,
  FormControl,
  FormErrorMessage,
  FormHelperText,
  FormLabel,
  HStack,
  Input,
  Textarea,
  Tooltip,
  VStack,
} from '@chakra-ui/react';
import { InfoIcon } from '@chakra-ui/icons';

/**
 * Credential fields for the source Airflow. The visible fields depend on the
 * selected platform. The URL is always shown first.
 *
 * onCredsChange receives a partial object matching the `set-source-creds`
 * action shape, e.g. `{ token: 'abc' }` or `{ region: 'us-west-2' }`.
 */
export default function SourceCredentialsForm({
  platform,
  url,
  token,
  login,
  password,
  impersonationChain,
  region,
  roleArn,
  environmentName,
  isTouched,
  credsTouched,
  isValidUrl,
  onUrlChange,
  onCredsChange,
}) {
  return (
    <VStack align="stretch" spacing={3}>
      <FormControl isInvalid={isTouched && !isValidUrl} isRequired>
        <HStack mb={1}>
          <FormLabel mb={0}>Source Airflow URL</FormLabel>
          <Tooltip
            label="The base URL of the Airflow webserver you are migrating from"
            placement="top"
            hasArrow
          >
            <InfoIcon color="gray.400" boxSize={3} cursor="help" />
          </Tooltip>
        </HStack>
        <Input
          size="sm"
          placeholder="https://source-airflow.example.com"
          value={url || ''}
          onChange={(e) => onUrlChange(e.target.value)}
        />
        <FormHelperText fontSize="xs">
          Paste the full webserver URL of the Airflow you are migrating from.
        </FormHelperText>
        <FormErrorMessage>Please enter a valid URL (http:// or https://).</FormErrorMessage>
      </FormControl>

      <Divider />

      {platform === 'astro' && (
        <FormControl isInvalid={credsTouched && !token} isRequired>
          <FormLabel mb={1}>Deployment API Token</FormLabel>
          <Input
            size="sm"
            type="password"
            autoComplete="off"
            placeholder="eyJhbGciOi..."
            value={token || ''}
            onChange={(e) => onCredsChange({ token: e.target.value })}
          />
          <FormHelperText fontSize="xs">
            An Organization, Workspace, or Deployment API token for the source Astro deployment.
          </FormHelperText>
          <FormErrorMessage>Token is required for Astro sources.</FormErrorMessage>
        </FormControl>
      )}

      {platform === 'oss' && (
        <>
          <Alert status="info" variant="left-accent" fontSize="xs" py={2}>
            <AlertIcon boxSize={3} />
            Provide a bearer token, or a username and password for HTTP Basic auth.
          </Alert>
          <FormControl>
            <FormLabel mb={1}>Bearer Token (optional)</FormLabel>
            <Input
              size="sm"
              type="password"
              autoComplete="off"
              value={token || ''}
              onChange={(e) => onCredsChange({ token: e.target.value })}
            />
          </FormControl>
          <FormControl>
            <FormLabel mb={1}>Username (HTTP Basic)</FormLabel>
            <Input
              size="sm"
              autoComplete="off"
              value={login || ''}
              onChange={(e) => onCredsChange({ login: e.target.value })}
            />
          </FormControl>
          <FormControl>
            <FormLabel mb={1}>Password (HTTP Basic)</FormLabel>
            <Input
              size="sm"
              type="password"
              autoComplete="off"
              value={password || ''}
              onChange={(e) => onCredsChange({ password: e.target.value })}
            />
          </FormControl>
        </>
      )}

      {platform === 'gcc' && (
        <>
          <Alert status="info" variant="left-accent" fontSize="xs" py={2}>
            <AlertIcon boxSize={3} />
            Composer uses Application Default Credentials from the worker&apos;s environment. Impersonation is optional.
          </Alert>
          <FormControl>
            <FormLabel mb={1}>Impersonation Chain (optional)</FormLabel>
            <Textarea
              size="sm"
              rows={3}
              placeholder={'intermediate-sa@project.iam.gserviceaccount.com\ntarget-sa@project.iam.gserviceaccount.com'}
              value={impersonationChain || ''}
              onChange={(e) => onCredsChange({ impersonationChain: e.target.value })}
            />
            <FormHelperText fontSize="xs">
              One service account per line. Last line is the target principal; any preceding entries are delegates.
            </FormHelperText>
          </FormControl>
        </>
      )}

      {platform === 'mwaa' && (
        <>
          <Alert status="info" variant="left-accent" fontSize="xs" py={2}>
            <AlertIcon boxSize={3} />
            MWAA uses IAM credentials from the worker&apos;s environment (boto3 default chain) against the specified region.
          </Alert>
          <FormControl isInvalid={credsTouched && !region} isRequired>
            <FormLabel mb={1}>AWS Region</FormLabel>
            <Input
              size="sm"
              placeholder="us-west-2"
              value={region || ''}
              onChange={(e) => onCredsChange({ region: e.target.value })}
            />
            <FormErrorMessage>Region is required for MWAA sources.</FormErrorMessage>
          </FormControl>
          <FormControl>
            <FormLabel mb={1}>MWAA Environment Name (optional)</FormLabel>
            <Input
              size="sm"
              placeholder="my-mwaa-env"
              value={environmentName || ''}
              onChange={(e) => onCredsChange({ environmentName: e.target.value })}
            />
            <FormHelperText fontSize="xs">
              Used to mint short-lived web-login tokens via CreateWebLoginToken.
            </FormHelperText>
          </FormControl>
          <FormControl>
            <FormLabel mb={1}>Assume Role ARN (optional)</FormLabel>
            <Input
              size="sm"
              placeholder="arn:aws:iam::123456789012:role/StarshipSource"
              value={roleArn || ''}
              onChange={(e) => onCredsChange({ roleArn: e.target.value })}
            />
          </FormControl>
        </>
      )}
    </VStack>
  );
}

SourceCredentialsForm.propTypes = {
  platform: PropTypes.oneOf(['astro', 'mwaa', 'gcc', 'oss']),
  url: PropTypes.string,
  token: PropTypes.string,
  login: PropTypes.string,
  password: PropTypes.string,
  impersonationChain: PropTypes.string,
  region: PropTypes.string,
  roleArn: PropTypes.string,
  environmentName: PropTypes.string,
  isTouched: PropTypes.bool.isRequired,
  credsTouched: PropTypes.bool.isRequired,
  isValidUrl: PropTypes.bool.isRequired,
  onUrlChange: PropTypes.func.isRequired,
  onCredsChange: PropTypes.func.isRequired,
};

SourceCredentialsForm.defaultProps = {
  platform: null,
  url: '',
  token: null,
  login: null,
  password: null,
  impersonationChain: '',
  region: '',
  roleArn: '',
  environmentName: '',
};
