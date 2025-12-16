import React, { memo, Suspense, lazy } from 'react';
import {
  createHashRouter,
  RouterProvider,
  Outlet,
  NavLink,
  Navigate,
} from 'react-router-dom';
import {
  Box, Button, Divider, Flex, Heading, HStack, Icon, Image, Spinner, Text, Tooltip,
} from '@chakra-ui/react';
import { GoRocket } from 'react-icons/go';
import PropTypes from 'prop-types';
import AstronomerLogo from './astronomer-logo.svg';

import { AppProvider, useSetupComplete } from './AppContext';
import { ROUTES } from './constants';
import './index.css';

// Lazy load page components for better initial bundle size
const SetupPage = lazy(() => import('./pages/SetupPage'));
const VariablesPage = lazy(() => import('./pages/VariablesPage'));
const ConnectionsPage = lazy(() => import('./pages/ConnectionsPage'));
const PoolsPage = lazy(() => import('./pages/PoolsPage'));
const EnvVarsPage = lazy(() => import('./pages/EnvVarsPage'));
const DAGHistoryPage = lazy(() => import('./pages/DAGHistoryPage'));
const TelescopePage = lazy(() => import('./pages/TelescopePage'));

// Loading fallback for lazy-loaded pages
function PageLoader() {
  return (
    <Box display="flex" justifyContent="center" alignItems="center" minH="200px">
      <Spinner size="lg" color="brand.500" thickness="3px" />
    </Box>
  );
}

function NavButtonComponent({
  to,
  label,
  isDisabled = false,
  disabledMessage = '',
}) {
  const button = (
    <Button
      as={isDisabled ? 'button' : NavLink}
      to={isDisabled ? undefined : to}
      variant="navLink"
      isDisabled={isDisabled}
    >
      {label}
    </Button>
  );

  if (isDisabled && disabledMessage) {
    return (
      <Tooltip label={disabledMessage} hasArrow placement="bottom">
        <Box as="span" display="inline-block">
          {button}
        </Box>
      </Tooltip>
    );
  }

  return button;
}

NavButtonComponent.propTypes = {
  to: PropTypes.string.isRequired,
  label: PropTypes.string.isRequired,
  isDisabled: PropTypes.bool,
  disabledMessage: PropTypes.string,
};

const NavButton = memo(NavButtonComponent);

const NAV_ITEMS = [
  { to: `/${ROUTES.SETUP}`, label: 'Setup', requiresSetup: false },
  { to: `/${ROUTES.VARIABLES}`, label: 'Variables', requiresSetup: true },
  { to: `/${ROUTES.CONNECTIONS}`, label: 'Connections', requiresSetup: true },
  { to: `/${ROUTES.POOLS}`, label: 'Pools', requiresSetup: true },
  { to: `/${ROUTES.ENV_VARS}`, label: 'Environment Variables', requiresSetup: true },
  { to: `/${ROUTES.DAGS}`, label: 'DAG History', requiresSetup: true },
];

const DISABLED_MESSAGE = 'Complete the Setup tab to configure your target Airflow instance before accessing migration features';

function AppLayout() {
  const isSetupComplete = useSetupComplete();

  return (
    <>
      <Flex
        as="nav"
        boxShadow="sm"
        borderBottomWidth="1px"
        borderColor="gray.200"
        overflowX="auto"
        overflowY="hidden"
        flexWrap={{ base: 'nowrap', lg: 'wrap' }}
        minH="12"
        css={{
          '&::-webkit-scrollbar': {
            height: 'var(--chakra-space-1)',
          },
          '&::-webkit-scrollbar-thumb': {
            background: 'var(--chakra-colors-gray-300)',
            borderRadius: 'var(--chakra-radii-sm)',
          },
        }}
      >
        {NAV_ITEMS.map(({ to, label, requiresSetup }) => (
          <NavButton
            key={to}
            to={to}
            label={label}
            isDisabled={requiresSetup && !isSetupComplete}
            disabledMessage={requiresSetup ? DISABLED_MESSAGE : ''}
          />
        ))}
      </Flex>
      <Box
        as="main"
        flex="1"
        minH="0"
        overflowY="auto"
        px={{ base: 2, md: 4, lg: 6 }}
        pt={{ base: 2, md: 4, lg: 6 }}
        pb={{ base: 10, md: 12 }}
      >
        <Box as="header" display="inline-block">
          <Heading as="h1" size="xl">
            Starship
            {' '}
            <Icon as={GoRocket} />
          </Heading>
          <HStack align="baseline" justify="flex-end">
            <Text color="gray.500">
              by
            </Text>
            <Image src={AstronomerLogo} alt="Astronomer" h="12px" />
          </HStack>
        </Box>
        <Divider my={3} />
        <Suspense fallback={<PageLoader />}>
          <Outlet />
        </Suspense>
      </Box>
    </>
  );
}

const router = createHashRouter([
  {
    path: '/',
    element: <AppLayout />,
    children: [
      { index: true, element: <Navigate to={ROUTES.SETUP} replace /> },
      { path: ROUTES.SETUP, element: <SetupPage /> },
      { path: ROUTES.VARIABLES, element: <VariablesPage /> },
      { path: ROUTES.CONNECTIONS, element: <ConnectionsPage /> },
      { path: ROUTES.POOLS, element: <PoolsPage /> },
      { path: ROUTES.ENV_VARS, element: <EnvVarsPage /> },
      { path: ROUTES.DAGS, element: <DAGHistoryPage /> },
      { path: ROUTES.TELESCOPE, element: <TelescopePage /> },
    ],
  },
]);

export default function App() {
  return (
    <AppProvider>
      <RouterProvider router={router} />
    </AppProvider>
  );
}
