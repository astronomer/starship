import React, { useEffect, useReducer } from 'react';
import {
  Box, Flex, Heading, Icon, Tab, TabIndicator, TabList, TabPanel, TabPanels, Tabs,
} from '@chakra-ui/react';
import { GoRocket } from 'react-icons/go';
import { createBrowserHistory } from 'history';
import VariablesPage from './pages/VariablesPage';
import ConnectionsPage from './pages/ConnectionsPage';
import PoolsPage from './pages/PoolsPage';
import EnvVarsPage from './pages/EnvVarsPage';
import DAGHistoryPage from './pages/DAGHistoryPage';
import SetupPage from './pages/SetupPage';
import {
  getInitialStateFromUrl, initialState, reducer, setHashState,
} from './State';
import './index.css';

export default function App() {
  const history = createBrowserHistory({
    window: window.parent,
  });
  const [state, dispatch] = useReducer(reducer, initialState, getInitialStateFromUrl);
  useEffect(() => setHashState(state, history), [state]);

  // noinspection JSCheckFunctionSignatures
  const handleTabsChange = (index) => dispatch({ type: 'set-tab', tab: index });

  return (
    <Flex
      direction="column"
      w="100%"
    >
      <Box as="header" padding="30px">
        <Heading color="#51504f" as="h1" size="2xl" noOfLines={1}>
          Starship
          {' '}
          <Icon as={GoRocket} />
        </Heading>
        <Heading size="sm" color="dimgrey" noOfLines={1}>By Astronomer</Heading>
      </Box>

      <Box as="nav" w="100%">
        <Tabs
          isLazy
          isFitted
          size="lg"
          colorScheme="dark"
          variant="unstyled"
          index={state.tab}
          onChange={handleTabsChange}
        >
          <TabList boxShadow="0 1px 0 0 #e2e8f0">
            <Tab>Setup</Tab>
            <Tab isDisabled={!state.isSetupComplete}>Variables</Tab>
            <Tab isDisabled={!state.isSetupComplete}>Connections</Tab>
            <Tab isDisabled={!state.isSetupComplete}>Pools</Tab>
            <Tab isDisabled={!state.isSetupComplete}>Environment Variables</Tab>
            <Tab isDisabled={!state.isSetupComplete}>DAG History</Tab>
          </TabList>
          <TabIndicator mt="-1.5px" height="2px" bg="blue.500" borderRadius="1px" />
          <TabPanels as="article">
            <TabPanel>
              <SetupPage
                state={state}
                dispatch={dispatch}
              />
            </TabPanel>
            <TabPanel>
              <VariablesPage />
            </TabPanel>
            <TabPanel>
              <ConnectionsPage />
            </TabPanel>
            <TabPanel>
              <PoolsPage />
            </TabPanel>
            <TabPanel>
              <EnvVarsPage />
            </TabPanel>
            <TabPanel>
              <DAGHistoryPage />
            </TabPanel>
          </TabPanels>
        </Tabs>
      </Box>
    </Flex>
  );
}
