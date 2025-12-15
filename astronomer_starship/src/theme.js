import { extendTheme } from '@chakra-ui/react';

/**
 * Astronomer Brand Theme for Starship
 * Based on 2025 Astronomer Creative Design System
 *
 * Color Palettes:
 * - Brand (Moonshot): Dark Purple/Navy - primary brand color
 * - Amethyst: Bright Purple - accent color for active states
 * - Success (Emerald): Green for success states
 * - Error (Ruby): Red for error states
 * - Warning (Amber): Orange for warning states
 * - Info (Sapphire): Blue for informational states
 */

const theme = extendTheme({
  colors: {
    // Brand: Moonshot - Dark Purple/Navy (primary brand palette)
    brand: {
      25: '#F8F8FF',
      50: '#EBE9F8',
      75: '#DCDBF1',
      100: '#CCCDE9',
      150: '#BDBFE2',
      200: '#A2A3D1',
      300: '#8887C0',
      400: '#6D6BAF',
      500: '#575293',
      600: '#413A77',
      700: '#2B215B',
      800: '#1E1D48',
      900: '#101935',
      950: '#0A1423',
    },

    // Amethyst: Purple for accents and tags
    amethyst: {
      50: '#FAF7FF',
      100: '#EDE1FF',
      200: '#DFCAFF',
      300: '#C69DFF',
      400: '#AF76FF',
      500: '#8A53D6',
      600: '#6B3AAD',
      700: '#4F2885',
      800: '#361A5C',
      900: '#1B0835',
    },

    // Emerald: Astronomer Green #0BCD93
    success: {
      50: '#F2FFF8',
      100: '#C6F7E1',
      200: '#99EEC9',
      300: '#4BDEA8',
      400: '#0BCD93',
      500: '#06AC80',
      600: '#068A6A',
      700: '#076953',
      800: '#08483A',
      900: '#012A21',
    },

    // Sapphire: Astronomer Blue #0AA6FF
    info: {
      50: '#F2FDFF',
      100: '#CBF4FF',
      200: '#A3EBFF',
      300: '#55CCFF',
      400: '#0AA6FF',
      500: '#0F74D5',
      600: '#1552AC',
      700: '#183A82',
      800: '#172758',
      900: '#061237',
    },

    // Ruby: Astronomer Red #FB3D31
    error: {
      50: '#FFE9E0',
      100: '#FFCFC1',
      200: '#FEB5A2',
      300: '#FC7B66',
      400: '#FB3D31',
      500: '#D22E16',
      600: '#AA2607',
      700: '#812101',
      800: '#591B00',
      900: '#3A1200',
    },

    // Amber: Yellow/Orange for warnings
    warning: {
      50: '#FFF6EB',
      100: '#FFE8CA',
      200: '#FED9A8',
      300: '#FEBF66',
      400: '#FDA92A',
      500: '#D38B1D',
      600: '#AA7016',
      700: '#805512',
      800: '#563A0F',
      900: '#3B2606',
    },
  },

  fonts: {
    heading: 'Albert Sans, Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif',
    body: 'Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif',
    mono: '"SF Mono", "Roboto Mono", "Courier New", monospace',
  },

  components: {
    Button: {
      defaultProps: {
        colorScheme: 'brand',
      },
      variants: {
        // Navigation link for navbar
        navLink: {
          borderRadius: 0,
          fontWeight: 'normal',
          color: 'brand.700',
          _hover: {
            bg: 'transparent',
            color: 'brand.600',
          },
          _disabled: { opacity: 0.4 },
          '&.active': {
            boxShadow: 'inset 0 -5px 0 0 var(--chakra-colors-amethyst-600)',
            fontWeight: 'semibold',
            color: 'brand.800',
          },
        },
      },
    },

    Input: {
      defaultProps: {
        focusBorderColor: 'brand.400',
      },
    },

    Select: {
      defaultProps: {
        focusBorderColor: 'brand.400',
      },
    },

    Heading: {
      baseStyle: {
        color: 'brand.700',
      },
    },

    Link: {
      baseStyle: {
        color: 'brand.400',
      },
    },
  },

  styles: {
    global: {
      html: {
        fontSize: '16px', // Override Airflow's 12px to standard 16px
      },
      body: {
        color: 'brand.700',
        fontSize: 'md', // Override Airflow's smaller body font
      },
      // Data table responsive styling
      '.data-table': {
        overflowX: 'auto',
        w: '100%',
      },
      // Airflow embedding container
      '.starship-main': {
        display: 'flex',
        flexDirection: 'column',
        minH: 0,
        maxH: '100vh',
        overflowY: 'auto',
      },
    },
  },

  config: {
    initialColorMode: 'light',
    useSystemColorMode: false,
  },
});

export default theme;
