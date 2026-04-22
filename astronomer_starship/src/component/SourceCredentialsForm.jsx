import PropTypes from 'prop-types';
import {
  Alert,
  AlertIcon,
  Box,
  Code,
  Divider,
  FormControl,
  FormErrorMessage,
  FormHelperText,
  FormLabel,
  HStack,
  Input,
  Text,
  Textarea,
  Tooltip,
  VStack,
} from '@chakra-ui/react';
import { InfoIcon } from '@chakra-ui/icons';

/**
 * Per-platform URL examples shown alongside the Source URL field.
 * Each entry: a placeholder for the input, plus one or more labelled
 * example strings rendered in a Code block below the helper text.
 */
const URL_EXAMPLES = {
  astro: {
    placeholder: 'https://cljabc123def.astronomer.run/abc12def',
    examples: [
      { label: 'Astro Cloud', value: 'https://cljabc123def.astronomer.run/abc12def' },
      { label: 'Astro Software', value: 'https://deployments.basedomain.com/release-name-1234/airflow' },
    ],
  },
  gcc: {
    placeholder: 'https://<hash>-dot-<region>.composer.googleusercontent.com',
    examples: [
      {
        label: 'Composer 2 / 3',
        value: 'https://e3954c2af6834e90aa5bf8xxxxxxxxxx-dot-us-central1.composer.googleusercontent.com',
      },
    ],
  },
  mwaa: {
    placeholder: 'https://<env-hash>.<cluster>.<region>.airflow.amazonaws.com',
    examples: [
      {
        label: 'MWAA',
        value: 'https://abcdef12-3456-7890-abcd-ef1234567890.c17.us-west-2.airflow.amazonaws.com',
      },
    ],
  },
  oss: {
    placeholder: 'https://airflow.example.com',
    examples: [
      { label: 'OSS (hostname)', value: 'https://airflow.example.com' },
      { label: 'OSS (host + port)', value: 'https://airflow.example.com:8080' },
    ],
  },
};

/**
 * Credential fields for the source Airflow. The visible fields depend on the
 * selected platform. The URL is always shown first.
 *
 * onCredsChange receives a partial object matching the `set-source-creds`
 * action shape, e.g. `{ token: 'abc' }` or `{ region: 'us-west-2' }`.
 */
export default function SourceCredentialsForm({
  platform = null,
  connId = '',
  url = '',
  token = null,
  login = null,
  password = null,
  impersonationChain = '',
  region = '',
  roleArn = '',
  environmentName = '',
  isTouched,
  credsTouched,
  isValidUrl,
  onConnIdChange,
  onUrlChange,
  onCredsChange,
}) {
  const urlHints = platform ? URL_EXAMPLES[platform] : null;

  // Normalize on input: strip trailing slashes and a trailing /home so users
  // can paste straight from the Airflow browser address bar without breaking
  // the /api/starship/info check.
  const handleUrlChange = (value) => {
    let cleaned = (value || '').trim();
    cleaned = cleaned.replace(/\/+$/, '');
    cleaned = cleaned.replace(/\/home$/, '');
    onUrlChange(cleaned);
  };

  return (
    <VStack align="stretch" spacing={3}>
      <FormControl isInvalid={isTouched && !isValidUrl} isRequired>
        <HStack mb={1}>
          <FormLabel mb={0}>Source Airflow URL</FormLabel>
          <Tooltip label="The base URL of the Airflow webserver you are migrating from" placement="top" hasArrow>
            <InfoIcon color="gray.400" boxSize={3} cursor="help" />
          </Tooltip>
        </HStack>
        <Input
          size="sm"
          placeholder={urlHints?.placeholder || 'https://source-airflow.example.com'}
          value={url || ''}
          onChange={(e) => handleUrlChange(e.target.value)}
          onBlur={(e) => handleUrlChange(e.target.value)}
        />
        <FormHelperText fontSize="xs">
          Paste the full webserver URL of the Airflow you are migrating from.
        </FormHelperText>
        <FormErrorMessage>Please enter a valid URL (http:// or https://).</FormErrorMessage>

        {urlHints && (
          <Box mt={2} p={2} bg="gray.50" borderRadius="md" borderWidth="1px" borderColor="gray.200">
            <Text fontSize="2xs" fontWeight="semibold" color="gray.600" mb={1}>
              Expected URL format
            </Text>
            <VStack align="stretch" spacing={1}>
              {urlHints.examples.map((ex) => (
                <Box key={ex.value}>
                  <Text fontSize="2xs" color="gray.500">
                    {ex.label}
                  </Text>
                  <Code fontSize="2xs" px={2} py={1} borderRadius="sm" display="block" wordBreak="break-all">
                    {ex.value}
                  </Code>
                </Box>
              ))}
            </VStack>
          </Box>
        )}
      </FormControl>

      <FormControl>
        <HStack mb={1}>
          <FormLabel mb={0}>Airflow Connection name</FormLabel>
          <Tooltip
            label="The Airflow Connection id these credentials will be saved under. Change only if you need multiple source connections side-by-side."
            placement="top"
            hasArrow
          >
            <InfoIcon color="gray.400" boxSize={3} cursor="help" />
          </Tooltip>
        </HStack>
        <Input
          size="sm"
          value={connId || ''}
          placeholder="starship_source"
          onChange={(e) => onConnIdChange(e.target.value)}
        />
        <FormHelperText fontSize="xs">
          Defaults to <Code fontSize="2xs">starship_source</Code>. Clicking Save below will create or update an Airflow
          Connection under this name. The wave engine and the template DAG read from this same connection.
        </FormHelperText>
      </FormControl>

      <Divider />

      {platform === 'astro' && (
        <FormControl isInvalid={credsTouched && !token} isRequired>
          <HStack mb={1}>
            <FormLabel mb={0}>Access Token</FormLabel>
            <Tooltip
              label="Organization, Workspace, or Personal access token. Deployment API Tokens do NOT work for the Starship API on Astro — Astro's edge proxy rejects them on plugin endpoints."
              placement="top"
              hasArrow
            >
              <InfoIcon color="gray.400" boxSize={3} cursor="help" />
            </Tooltip>
          </HStack>
          <Input
            size="sm"
            type="password"
            autoComplete="off"
            placeholder="eyJhbGciOi..."
            value={token || ''}
            onChange={(e) => onCredsChange({ token: e.target.value })}
          />
          <FormHelperText fontSize="xs">
            Use an <strong>Organization</strong>, <strong>Workspace</strong>, or <strong>Personal</strong> access token
            from the source Astro. <strong>Deployment API Tokens do not work</strong> — Astro&apos;s edge proxy (Istio)
            rejects them on the Starship plugin endpoints.
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
            MWAA uses IAM credentials from the worker&apos;s environment (boto3 default chain) against the specified
            region.
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
  connId: PropTypes.string,
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
  onConnIdChange: PropTypes.func.isRequired,
  onUrlChange: PropTypes.func.isRequired,
  onCredsChange: PropTypes.func.isRequired,
};
