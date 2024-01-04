import {
  Box,
  Divider,
  VStack,
  Text,
  InputGroup,
  Input,
  InputRightAddon,
  InputLeftAddon,
  FormLabel,
  FormControl,
  Switch,
  HStack,
  Link,
  SlideFade,
  Button,
  Fade,
  FormErrorMessage,
  FormHelperText,
  InputRightElement,
} from '@chakra-ui/react';
import React from 'react';
import PropTypes from 'prop-types';
import { CheckIcon, ExternalLinkIcon, SpinnerIcon } from '@chakra-ui/icons';
import { getTargetUrlFromParts, tokenUrlFromAirflowUrl } from '../util';

export default function SetupPage({ state, dispatch }) {
  return (
    <>
      <Box width="100%" margin="30px">
        <Text fontSize="xl">Starship is a utility to migrate Airflow metadata between instances</Text>
      </Box>
      <Divider />
      <VStack width="60%" display="flex" alignItems="center">
        <Box id="setup-form" width="100%" margin="30px" alignItems="left">

          {/* ==== PRODUCT SELECTOR ==== */}
          <VStack spacing="30px">
            <FormControl className="setup-form-field" isRequired>
              <HStack width="50%">
                <FormLabel htmlFor="is-astro">
                  Astronomer Product
                </FormLabel>
                <HStack>
                  <Text>Software</Text>
                  <Switch
                    defaultChecked
                    id="is-astro"
                    size="lg"
                    isChecked={state.isAstro}
                    onChange={() => dispatch({ type: 'toggle-is-astro' })}
                  />
                  <Text>Hosted</Text>
                </HStack>
                <Fade in={!state.isProductSelected}>
                  <Button
                    isDisabled={state.isProductSelected}
                    onClick={() => dispatch({ type: 'set-is-product-selected' })}
                  >
                    Next
                  </Button>
                </Fade>
              </HStack>
              <FormHelperText>
                The Astronomer Product you are
                <Text as="i"> migrating to.</Text>
              </FormHelperText>

            </FormControl>

            {/* ==== URL INPUT ==== */}
            <FormControl className="setup-form-field" isInvalid={state.isTouched && !state.isValidUrl} isRequired>
              <SlideFade in={state.isProductSelected}>
                <FormLabel>Airflow URL</FormLabel>
                { state.isAstro ? (
                // Astro URL Template: https://claaabbbcccddd.astronomer.run/aabbccdd/
                  <InputGroup size="sm">
                    <InputLeftAddon>https://</InputLeftAddon>
                    <Input
                      id="astroUrlOrgPart"
                      className="astroUrl"
                      placeholder="claaabbbcccddd"
                      errorBorderColor="red.300"
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
                    <Input
                      className="astroUrl"
                      placeholder="aabbccdd"
                      errorBorderColor="red.300"
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
                ) : (
                // Software URL Template: https://astro.basedomain.com/space-name-1234/airflow/
                  <InputGroup size="sm">
                    <InputLeftAddon>https://</InputLeftAddon>
                    <Input
                      className="astroUrl"
                      placeholder="astro.basedomain.com"
                      errorBorderColor="red.300"
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
                    <Input
                      className="astroUrl"
                      placeholder="space-name-1234"
                      errorBorderColor="red.300"
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
                )}
                <FormHelperText>
                  Enter the URL of the Airflow you are migrating to.
                </FormHelperText>
                <FormErrorMessage>Please fill both parts.</FormErrorMessage>
              </SlideFade>
            </FormControl>

            {/* ==== TOKEN INPUT ==== */}
            <FormControl isInvalid={state.isTokenTouched && !state.token} className="setup-form-field">
              <SlideFade in={state.isTouched && state.isProductSelected}>
                <FormLabel>Token</FormLabel>
                <InputGroup>
                  <Input
                    value={state.token}
                    isInvalid={state.isTokenTouched && !state.token}
                    placeholder="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMj..."
                    onChange={(e) => dispatch({ type: 'set-token', token: e.target.value })}
                  />
                  {state.isTokenTouched && state.token ? (
                    <InputRightElement>
                      <CheckIcon color="green.500" />
                    </InputRightElement>
                  ) : null}
                </InputGroup>
                {state.isAstro ? (
                  <FormHelperText>
                    Provide a token:
                    {' '}
                    <Link href="https://docs.astronomer.io/astro/organization-api-tokens#create-an-organization-api-token">Organization</Link>
                    ,
                    {' '}
                    <Link href="https://docs.astronomer.io/astro/workspace-api-tokens#create-a-workspace-api-token">Workspace</Link>
                    ,
                    {' '}
                    <Link href={tokenUrlFromAirflowUrl(state.targetUrl)}>Personal</Link>
                    .
                  </FormHelperText>
                ) : (
                  <FormHelperText>
                    Provide a token:
                    {' '}
                    <Link href="https://docs.astronomer.io/software/manage-workspaces#service-accounts">Workspace</Link>
                    ,
                    {' '}
                    <Link href="https://docs.astronomer.io/software/ci-cd#step-1-create-a-service-account">Deployment</Link>
                    {state.targetUrl.startsWith('https://') && state.isValidUrl ? (
                      <>
                        ,
                        {' '}
                        <Link href={tokenUrlFromAirflowUrl(state.targetUrl)}>Personal</Link>
                      </>
                    ) : null}
                    .
                  </FormHelperText>
                )}
                <FormErrorMessage>Please input a token.</FormErrorMessage>
              </SlideFade>
            </FormControl>

            {/* ==== CHECK AIRFLOW AND STARSHIP ==== */}
            <FormControl className="setup-form-field">
              <SlideFade in={state.targetUrl.startsWith('http') && state.isValidUrl && state.isProductSelected}>
                <FormLabel>
                  Target Airflow Link:
                </FormLabel>
                <HStack spacing="30px">
                  <Link href={state.targetUrl}>
                    {state.targetUrl}
                    <ExternalLinkIcon mx="2px" />
                  </Link>
                  <Text>
                    Is Airflow?
                  </Text>
                  {/* TODO - SHOW IF AIRFLOW IS FOUND */}
                  <SpinnerIcon margin="0 20px" />

                  {/* TODO - SHOW IF STARSHIP IS FOUND */}
                  <Text>
                    Has Starship?
                  </Text>
                  <SpinnerIcon />

                </HStack>
              </SlideFade>
            </FormControl>
          </VStack>
        </Box>
      </VStack>
    </>
  );
}
// eslint-disable-next-line react/forbid-prop-types
SetupPage.propTypes = { state: PropTypes.object.isRequired, dispatch: PropTypes.func.isRequired };
