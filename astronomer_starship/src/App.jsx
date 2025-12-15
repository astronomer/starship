import React, { memo } from 'react';
import {
  createHashRouter,
  RouterProvider,
  Outlet,
  NavLink,
  Navigate,
} from 'react-router-dom';
import {
  Box, Button, Divider, Flex, Heading, HStack, Icon, Image, Text, Tooltip,
} from '@chakra-ui/react';
import { GoRocket } from 'react-icons/go';
import PropTypes from 'prop-types';
import AstronomerLogo from './astronomer-logo.svg';

import { AppProvider, useSetupComplete } from './AppContext';
import SetupPage from './pages/SetupPage';
import VariablesPage from './pages/VariablesPage';
import ConnectionsPage from './pages/ConnectionsPage';
import PoolsPage from './pages/PoolsPage';
import EnvVarsPage from './pages/EnvVarsPage';
import DAGHistoryPage from './pages/DAGHistoryPage';
import TelescopePage from './pages/TelescopePage';
import './index.css';

const NavButton = memo(function NavButton({
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
});

NavButton.propTypes = {
  to: PropTypes.string.isRequired,
  label: PropTypes.string.isRequired,
  isDisabled: PropTypes.bool,
  disabledMessage: PropTypes.string,
};

const NAV_ITEMS = [
  { to: '/setup', label: 'Setup', requiresSetup: false },
  { to: '/variables', label: 'Variables', requiresSetup: true },
  { to: '/connections', label: 'Connections', requiresSetup: true },
  { to: '/pools', label: 'Pools', requiresSetup: true },
  { to: '/env', label: 'Environment Variables', requiresSetup: true },
  { to: '/dags', label: 'DAG History', requiresSetup: true },
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
        <Outlet />
      </Box>
    </>
  );
}

const router = createHashRouter([
  {
    path: '/',
    element: <AppLayout />,
    children: [
      { index: true, element: <Navigate to="/setup" replace /> },
      { path: 'setup', element: <SetupPage /> },
      { path: 'variables', element: <VariablesPage /> },
      { path: 'connections', element: <ConnectionsPage /> },
      { path: 'pools', element: <PoolsPage /> },
      { path: 'env', element: <EnvVarsPage /> },
      { path: 'dags', element: <DAGHistoryPage /> },
      { path: 'telescope', element: <TelescopePage /> },
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
