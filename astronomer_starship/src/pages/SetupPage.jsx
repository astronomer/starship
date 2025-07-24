import {
  Box,
  Button,
  Divider,
  Fade,
  FormControl,
  FormErrorMessage,
  FormHelperText,
  FormLabel,
  HStack,
  Input,
  InputGroup,
  InputLeftAddon,
  InputRightAddon,
  InputRightElement,
  Link,
  SlideFade,
  Spacer,
  Switch,
  Text,
  VStack,
} from '@chakra-ui/react';
import React, { useEffect } from 'react';
import PropTypes from 'prop-types';
import { CheckIcon, ExternalLinkIcon, RepeatIcon, } from '@chakra-ui/icons';
import { IoTelescopeOutline } from 'react-icons/io5';
import { NavLink } from 'react-router-dom';
import { getHoustonRoute, getTargetUrlFromParts, proxyHeaders, proxyUrl, tokenUrlFromAirflowUrl } from '../util';
import ValidatedUrlCheckbox from '../component/ValidatedUrlCheckbox';
import axios from "axios";
import { getWorkspaceDeploymentsQuery } from "../constants.js";

export default function SetupPage({ state, dispatch }) {
  // Get the workspace ID & etc. if it's software and setup is completed
  useEffect(
    () => {
      if (
        state.isSetupComplete && // setup is completed
        !state.isAstro &&  // it's Software
        !(state.releaseName && state.workspaceId && state.deploymentId) // one or more of three isn't set
      ){
        axios.post(
          proxyUrl(getHoustonRoute(state.urlOrgPart)),
          {
            operationName: "workspaces",
            query: getWorkspaceDeploymentsQuery,
            variables: {}
          },
          {
            headers: proxyHeaders(state.token)
          }
        )
        .then((res) => {
          let found = false;
          for (let workspace of res.data?.data?.workspaces) {
            if (found) break;
            for (let deployment of workspace.deployments) {
              if (found) break;
              if (deployment.releaseName === state.urlDeploymentPart) {
                dispatch({
                  type: 'set-software-info',
                  deploymentId: deployment.id,
                  releaseName: deployment.releaseName,
                  workspaceId: workspace.id
                });
              }
            }
          }
          res.data?.data?.workspaces
        })
        .catch((err) => {});
      }
    },
    [state],
  );

  return (
    <Box>
      <HStack>
        <Text fontSize="xl">Starship is a utility to migrate Airflow metadata between instances</Text>
        <Spacer />
        <Button
          size="sm"
          leftIcon={<IoTelescopeOutline />}
          as={NavLink}
          to="/telescope"
        >
          Telescope
        </Button>
        <Button
          size="sm"
          leftIcon={<RepeatIcon />}
          onClick={() => dispatch({ type: 'reset' })}
        >
          Reset
        </Button>
      </HStack>
      <Divider marginY="5px" />
      <VStack width="60%" display="flex" alignItems="center">
        <Box id="setup-form" width="100%" margin="0 30px" alignItems="left">

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
                  <Text>Astro</Text>
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
                {state.isAstro ? (
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
                  // Software URL Template: https://deployments.basedomain.com/space-name-1234/airflow/home
                  <InputGroup size="sm">
                    <InputLeftAddon>https://deployments.</InputLeftAddon>
                    <Input
                      className="astroUrl"
                      placeholder="basedomain.com"
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
                      placeholder="release-name-1234"
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
                    type="password"
                    value={state.token || ''}
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
                    <Link
                      isExternal
                      href="https://docs.astronomer.io/astro/organization-api-tokens#create-an-organization-api-token"
                    >
                      Organization
                      <ExternalLinkIcon mx="2px" />
                    </Link>
                    ,
                    {' '}
                    <Link
                      isExternal
                      href="https://docs.astronomer.io/astro/workspace-api-tokens#create-a-workspace-api-token"
                    >
                      Workspace
                      <ExternalLinkIcon mx="2px" />
                    </Link>
                    ,
                    {' '}
                    <Link
                      isExternal
                      href={tokenUrlFromAirflowUrl(state.targetUrl)}
                    >
                      Personal
                      <ExternalLinkIcon mx="2px" />
                    </Link>
                    .
                  </FormHelperText>
                ) : (
                  <FormHelperText>
                    Provide a token:
                    {' '}
                    <Link
                      isExternal
                      href="https://docs.astronomer.io/software/manage-workspaces#service-accounts"
                    >
                      Workspace
                      <ExternalLinkIcon mx="2px" />
                    </Link>
                    ,
                    {' '}
                    <Link
                      isExternal
                      href="https://docs.astronomer.io/software/ci-cd#step-1-create-a-service-account"
                    >
                      Deployment
                      <ExternalLinkIcon mx="2px" />
                    </Link>
                    {state.targetUrl.startsWith('https://') && state.isValidUrl ? (
                      <>
                        ,
                        {' '}
                        <Link
                          isExternal
                          href={tokenUrlFromAirflowUrl(state.targetUrl)}
                        >
                          Personal
                          <ExternalLinkIcon mx="2px" />

                        </Link>
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
                <Link isExternal href={state.targetUrl}>
                  Target Airflow
                  <ExternalLinkIcon mx="2px" />
                </Link>
                {state.targetUrl.startsWith('http') && state.token && state.isValidUrl && state.isProductSelected
                  ? (
                    <HStack>
                      <ValidatedUrlCheckbox
                        colorScheme="green"
                        // size="lg"
                        text="Airflow"
                        valid={state.isAirflow}
                        setValid={(value) => dispatch({ type: 'set-is-airflow', isAirflow: value })}
                        url={`${state.targetUrl}/api/v1/health`}
                        token={state.token}
                      />
                      <ValidatedUrlCheckbox
                        colorScheme="green"
                        // size="lg"
                        text="Starship"
                        valid={state.isStarship}
                        setValid={(value) => dispatch({ type: 'set-is-starship', isStarship: value })}
                        url={`${state.targetUrl}/api/starship/info`}
                        token={state.token}
                      />
                    </HStack>
                  ) : null}
              </SlideFade>
            </FormControl>
          </VStack>
        </Box>
      </VStack>
    </Box>
  );
}
// eslint-disable-next-line react/forbid-prop-types
SetupPage.propTypes = { state: PropTypes.object.isRequired, dispatch: PropTypes.func.isRequired };
