import React, { memo } from 'react';
import {
  createHashRouter,
  RouterProvider,
  Outlet,
  NavLink,
  Navigate,
} from 'react-router-dom';
import {
  Box, Button, Divider, Flex, Heading, Icon, Tooltip,
} from '@chakra-ui/react';
import { GoRocket } from 'react-icons/go';
import PropTypes from 'prop-types';

import { AppProvider, useSetupComplete } from './AppContext';
import SetupPage from './pages/SetupPage';
import VariablesPage from './pages/VariablesPage';
import ConnectionsPage from './pages/ConnectionsPage';
import PoolsPage from './pages/PoolsPage';
import EnvVarsPage from './pages/EnvVarsPage';
import DAGHistoryPage from './pages/DAGHistoryPage';
import TelescopePage from './pages/TelescopePage';
import './index.css';

// ============================================================================
// NAV BUTTON - Memoized for performance
// ============================================================================
const NavButton = memo(function NavButton({
  to, label, isDisabled, disabledMessage,
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

NavButton.defaultProps = {
  isDisabled: false,
  disabledMessage: '',
};

// ============================================================================
// NAV CONFIG - Define navigation items once
// ============================================================================
const NAV_ITEMS = [
  { to: '/setup', label: 'Setup', requiresSetup: false },
  { to: '/variables', label: 'Variables', requiresSetup: true },
  { to: '/connections', label: 'Connections', requiresSetup: true },
  { to: '/pools', label: 'Pools', requiresSetup: true },
  { to: '/env', label: 'Environment Variables', requiresSetup: true },
  { to: '/dags', label: 'DAG History', requiresSetup: true },
];

const DISABLED_MESSAGE = 'Complete the Setup tab to configure your target Airflow instance before accessing migration features';

// ============================================================================
// APP LAYOUT
// ============================================================================
function AppLayout() {
  const isSetupComplete = useSetupComplete();

  return (
    <>
      <Flex as="nav" boxShadow="sm" borderBottomWidth="1px" borderColor="gray.200">
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
      <Box as="main" className="starship-page">
        <Box as="header" mb={2}>
          <Heading as="h1" size="xl" color="moonshot.700">
            Starship
            {' '}
            <Icon as={GoRocket} />
          </Heading>
          <Heading as="h2" size="xs" color="gray.500" fontWeight="normal">
            By Astronomer
          </Heading>
        </Box>
        <Divider my={3} />
        <Outlet />
      </Box>
    </>
  );
}

// ============================================================================
// ROUTER - Created once at module level
// ============================================================================
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

// ============================================================================
// APP COMPONENT
// ============================================================================
export default function App() {
  return (
    <AppProvider>
      <RouterProvider router={router} />
    </AppProvider>
  );
}
