/* eslint-disable no-nested-ternary */
import React, { useEffect, useReducer } from 'react';
import {
  Box, Button, Divider, Flex, Heading, Icon,
} from '@chakra-ui/react';
import { GoRocket } from 'react-icons/go';
import {
  Outlet, NavLink, Route, Navigate, createHashRouter, createRoutesFromElements,
} from 'react-router-dom';
import { RouterProvider } from 'react-router';
import VariablesPage from './pages/VariablesPage';
import ConnectionsPage from './pages/ConnectionsPage';
import PoolsPage from './pages/PoolsPage';
import EnvVarsPage from './pages/EnvVarsPage';
import DAGHistoryPage from './pages/DAGHistoryPage';
import SetupPage from './pages/SetupPage';
import {
  getInitialState, initialState, reducer,
} from './State';
import './index.css';
import AppLoading from './component/AppLoading';

export default function App() {
  const [state, dispatch] = useReducer(reducer, initialState, getInitialState);
  useEffect(() => {
    localStorage.setItem('state', JSON.stringify(state));
  }, [state]);
  const router = createHashRouter(
    createRoutesFromElements(
      <Route
        path="/"
        element={(
          <>
            <Flex
              as="nav"
              boxShadow="0 1px 0 0 #e2e8f0"
              id="starship-navbar"
            >
              <Button
                className={({ isActive, isPending }) => (isPending ? 'pending' : isActive ? 'active' : '')}
                w="100%"
                as={NavLink}
                to="/setup"
              >
                Setup
              </Button>
              <Button
                className={({ isActive, isPending }) => (isPending ? 'pending' : isActive ? 'active' : '')}
                w="100%"
                isDisabled={!state.isSetupComplete}
                as={NavLink}
                style={!state.isSetupComplete ? { pointerEvents: 'none' } : {}}
                to="/variables"
              >
                Variables
              </Button>
              <Button
                className={({ isActive, isPending }) => (isPending ? 'pending' : isActive ? 'active' : '')}
                w="100%"
                isDisabled={!state.isSetupComplete}
                style={!state.isSetupComplete ? { pointerEvents: 'none' } : {}}
                as={NavLink}
                to="/connections"
              >
                Connections
              </Button>
              <Button
                className={({ isActive, isPending }) => (isPending ? 'pending' : isActive ? 'active' : '')}
                w="100%"
                isDisabled={!state.isSetupComplete}
                style={!state.isSetupComplete ? { pointerEvents: 'none' } : {}}
                as={NavLink}
                to="/pools"
              >
                Pools
              </Button>
              <Button
                className={({ isActive, isPending }) => (isPending ? 'pending' : isActive ? 'active' : '')}
                w="100%"
                isDisabled={!state.isSetupComplete}
                style={!state.isSetupComplete ? { pointerEvents: 'none' } : {}}
                as={NavLink}
                to="/env"
              >
                Environment Variables
              </Button>
              <Button
                className={({ isActive, isPending }) => (isPending ? 'pending' : isActive ? 'active' : '')}
                w="100%"
                isDisabled={!state.isSetupComplete}
                style={!state.isSetupComplete ? { pointerEvents: 'none' } : {}}
                as={NavLink}
                to="/dags"
              >
                DAG History
              </Button>
            </Flex>
            <Box as="main" className="starship-page">
              <Box as="header" className="starship-logo">
                <Heading color="Gray.600" as="h1" size="2xl" noOfLines={1}>
                  Starship
                  {' '}
                  <Icon as={GoRocket} />
                </Heading>
                <Heading color="Gray.400" size="sm" noOfLines={1}>By Astronomer</Heading>
              </Box>
              <Divider marginY="5px" />
              <AppLoading />
              <Outlet />
            </Box>
          </>
        )}
      >
        <Route index element={<Navigate to="/setup" replace />} />
        <Route path="setup" element={<SetupPage key="setup" state={state} dispatch={dispatch} />} />
        <Route path="variables" element={<VariablesPage key="variables" state={state} dispatch={dispatch} />} />
        <Route path="connections" element={<ConnectionsPage key="connections" state={state} dispatch={dispatch} />} />
        <Route path="pools" element={<PoolsPage key="pools" state={state} dispatch={dispatch} />} />
        <Route path="env" element={<EnvVarsPage key="env-vars" state={state} dispatch={dispatch} />} />
        <Route path="dags" element={<DAGHistoryPage key="dag-history" state={state} dispatch={dispatch} />} />
      </Route>,
    ),
  );
  return (
    <RouterProvider router={router} />
  );
}
