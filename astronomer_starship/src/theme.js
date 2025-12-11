import { extendTheme } from '@chakra-ui/react';

/**
 * Astronomer Brand Theme for Starship
 * Based on 2025 Astronomer Creative Design System
 *
 * Primary Palette:
 * - Moonshot (Dark Purple/Navy for backgrounds and text)
 * - Amethyst (Bright Purple - primary accent)
 * - Sapphire (Blue)
 * - Emerald (Green)
 * - Ruby, Topaz, Amber, Rose Quartz (tertiary accents)
 */

const theme = extendTheme({
  // Semantic tokens for consistent, maintainable styling
  semanticTokens: {
    colors: {
      'text.primary': 'moonshot.700',
      'text.secondary': 'gray.600',
      'text.muted': 'gray.500',
      'text.inverse': 'white',
      'bg.page': 'white',
      'bg.subtle': 'moonshot.50',
      'bg.muted': 'gray.50',
      'border.default': 'gray.200',
      'border.muted': 'gray.300',
      'accent.primary': 'brand.400',
      'accent.secondary': 'success.400',
      'nav.active': 'brand.400',
    },
  },

  colors: {
    // Astronomer Primary: Amethyst Purple #AF76FF (Main Accent Color)
    brand: {
      50: '#FAF7FF',
      100: '#EDE1FF',
      200: '#DFCAFF',
      300: '#C69DFF',
      400: '#AF76FF', // Amethyst 400 - Main Astronomer Purple (Accent)
      500: '#8A53D6',
      600: '#6B3AAD',
      700: '#4F2885',
      800: '#361A5C',
      900: '#1B0835',
    },

    // Moonshot: Dark Purple/Navy (Primary Palette - for backgrounds and text)
    moonshot: {
      25: '#F8F8FF',
      50: '#EBE9F8', // Moonshot 50 - Light background
      75: '#DCDBF1',
      100: '#CCCDE9',
      150: '#BDBFE2',
      200: '#A2A3D1',
      300: '#8887C0',
      400: '#6D6BAF',
      500: '#575293',
      600: '#413A77',
      700: '#2B215B', // Moonshot 700 - Primary Text
      800: '#1E1D48',
      900: '#101935', // Moonshot 900 - Dark Backgrounds
      950: '#0A1423', // Moonshot 950 - Darkest Background
    },

    // Emerald: Astronomer Green #0BCD93 (Success/Active states)
    success: {
      50: '#F2FFF8',
      100: '#C6F7E1',
      200: '#99EEC9',
      300: '#4BDEA8',
      400: '#0BCD93', // Emerald 400 - Success/Active states
      500: '#06AC80',
      600: '#068A6A',
      700: '#076953',
      800: '#08483A',
      900: '#012A21',
    },

    // Sapphire: Astronomer Blue #0AA6FF (Info states)
    info: {
      50: '#F2FDFF',
      100: '#CBF4FF',
      200: '#A3EBFF',
      300: '#55CCFF',
      400: '#0AA6FF', // Sapphire 400 - Info states
      500: '#0F74D5',
      600: '#1552AC',
      700: '#183A82',
      800: '#172758',
      900: '#061237',
    },

    // Ruby: Astronomer Red #FB3D31 (Error/Delete states)
    error: {
      50: '#FFE9E0',
      100: '#FFCFC1',
      200: '#FEB5A2',
      300: '#FC7B66',
      400: '#FB3D31', // Ruby 400 - Error/Delete states
      500: '#D22E16',
      600: '#AA2607',
      700: '#812101',
      800: '#591B00',
      900: '#3A1200',
    },

    // Amber: Yellow/Orange (Warning states)
    warning: {
      50: '#FFF6EB',
      100: '#FFE8CA',
      200: '#FED9A8',
      300: '#FEBF66',
      400: '#FDA92A', // Amber 400 - Warning states
      500: '#D38B1D',
      600: '#AA7016',
      700: '#805512',
      800: '#563A0F',
      900: '#3B2606',
    },

    // Topaz: Orange accent (Tertiary)
    topaz: {
      50: '#FFEEE1',
      100: '#FFD9BD',
      200: '#FFC399',
      300: '#FF9D61',
      400: '#FF792E', // Topaz 400
      500: '#D85D15',
      600: '#B14807',
      700: '#8A3700',
      800: '#632800',
      900: '#431B00',
    },

    // Rose Quartz: Pink accent (Tertiary)
    roseQuartz: {
      50: '#FCE8F3',
      100: '#FBC3DD',
      200: '#F8A0C3',
      300: '#F36FA5',
      400: '#F03C78', // Rose Quartz 400
      500: '#C82256',
      600: '#A10F36',
      700: '#790724',
      800: '#55051D',
      900: '#360312',
    },

    // Gray scale (enhanced for better UI)
    gray: {
      50: '#f9fafb',
      100: '#f3f4f6',
      200: '#e5e7eb',
      300: '#d1d5db',
      400: '#9ca3af',
      500: '#6b7280',
      600: '#4b5563',
      700: '#374151',
      800: '#1f2937',
      900: '#111827',
    },

    // Semantic aliases
    primary: {
      50: '#f7f0ff',
      400: '#AF76FF', // Amethyst
      500: '#9d5aff',
      700: '#2B215B', // Moonshot 700
      900: '#101935', // Moonshot 900
    },
  },

  fonts: {
    heading: 'Albert Sans, Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif',
    body: 'Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif',
    mono: '"SF Mono", "Roboto Mono", "Courier New", monospace',
  },

  fontSizes: {
    xs: '0.75rem', // 12px
    sm: '0.875rem', // 14px
    md: '1rem', // 16px
    lg: '1.125rem', // 18px
    xl: '1.25rem', // 20px
    '2xl': '1.375rem', // 22px
    '3xl': '1.75rem', // 28px
    '4xl': '2.25rem', // 36px
    '5xl': '3rem', // 48px
  },

  fontWeights: {
    normal: 400,
    medium: 500,
    semibold: 600,
    bold: 700,
  },

  lineHeights: {
    tight: 1.14, // 32px/28px from Figma
    base: 1.18, // 26px/22px from Figma
    normal: 1.5,
    relaxed: 1.78, // 64px/36px from Figma
  },

  radii: {
    sm: '0.25rem', // 4px
    base: '0.375rem', // 6px
    md: '0.5rem', // 8px
    lg: '0.75rem', // 12px
    xl: '1rem', // 16px
    full: '9999px',
  },

  shadows: {
    sm: '0 1px 2px 0 rgba(10, 20, 35, 0.05)',
    base: '0 1px 3px 0 rgba(10, 20, 35, 0.1), 0 1px 2px 0 rgba(10, 20, 35, 0.06)',
    md: '0 4px 6px -1px rgba(10, 20, 35, 0.1), 0 2px 4px -1px rgba(10, 20, 35, 0.06)',
    lg: '0 10px 15px -3px rgba(10, 20, 35, 0.1), 0 4px 6px -2px rgba(10, 20, 35, 0.05)',
    xl: '0 20px 25px -5px rgba(10, 20, 35, 0.1), 0 10px 10px -5px rgba(10, 20, 35, 0.04)',
  },

  components: {
    Button: {
      baseStyle: {
        fontWeight: 'semibold',
        borderRadius: 'md',
      },
      sizes: {
        sm: {
          fontSize: 'sm',
          px: 3,
          py: 2,
          h: 8,
        },
        md: {
          fontSize: 'md',
          px: 4,
          py: 2,
          h: 10,
        },
      },
      variants: {
        solid: {
          bg: 'brand.400',
          color: 'white',
          _hover: {
            bg: 'brand.500',
            _disabled: {
              bg: 'gray.300',
            },
          },
          _active: {
            bg: 'brand.600',
          },
        },
        outline: {
          borderColor: 'currentColor',
          _hover: {
            bg: 'moonshot.50',
          },
        },
        ghost: {
          _hover: {
            bg: 'moonshot.50',
          },
        },
        // Navigation link button variant for navbar
        navLink: {
          border: 'none',
          borderRadius: 0,
          fontWeight: 'normal',
          whiteSpace: 'nowrap',
          lineHeight: 1.2,
          py: 2,
          px: 4,
          h: 'auto',
          minH: 12,
          _hover: {
            bg: 'transparent',
          },
          _disabled: {
            opacity: 0.4,
            cursor: 'not-allowed',
          },
          // Active state styling (for react-router NavLink)
          '&.active': {
            boxShadow: 'inset 0 -5px 0 0 var(--chakra-colors-brand-400)',
            fontWeight: 600,
          },
        },
      },
      defaultProps: {
        size: 'sm',
        variant: 'outline',
      },
    },

    Input: {
      sizes: {
        sm: {
          fontSize: 'sm',
          px: 3,
          py: 2,
        },
      },
      variants: {
        outline: {
          field: {
            borderColor: 'gray.300',
            _hover: {
              borderColor: 'gray.400',
            },
            _focus: {
              borderColor: 'brand.400',
              boxShadow: '0 0 0 1px var(--chakra-colors-brand-400)',
            },
          },
        },
      },
      defaultProps: {
        size: 'sm',
      },
    },

    Card: {
      baseStyle: {
        container: {
          borderRadius: 'lg',
          boxShadow: 'sm',
          bg: 'white',
        },
      },
      variants: {
        elevated: {
          container: {
            boxShadow: 'md',
          },
        },
      },
    },

    Heading: {
      baseStyle: {
        fontWeight: 'bold',
        color: 'moonshot.700',
        letterSpacing: '-0.02em',
      },
      sizes: {
        xs: {
          fontSize: 'xl',
          lineHeight: 'base',
        },
        sm: {
          fontSize: '2xl',
          lineHeight: 'base',
        },
        md: {
          fontSize: '3xl',
          lineHeight: 'tight',
        },
        lg: {
          fontSize: '4xl',
          lineHeight: 'relaxed',
        },
      },
    },

    Text: {
      baseStyle: {
        color: 'gray.700',
      },
    },

    Badge: {
      baseStyle: {
        fontWeight: 'semibold',
        fontSize: 'xs',
        textTransform: 'uppercase',
      },
      variants: {
        solid: {
          bg: 'brand.400',
          color: 'white',
        },
      },
    },

    Progress: {
      baseStyle: {
        filledTrack: {
          bg: 'brand.400',
        },
      },
      variants: {
        brand: {
          filledTrack: {
            bg: 'brand.400',
          },
        },
        success: {
          filledTrack: {
            bg: 'success.400',
          },
        },
      },
    },

    Switch: {
      baseStyle: {
        track: {
          _checked: {
            bg: 'brand.400',
          },
        },
      },
      variants: {
        success: {
          track: {
            _checked: {
              bg: 'success.400',
            },
          },
        },
        // Astro product toggle: Software (green) vs Hosted (purple)
        astroToggle: {
          track: {
            bg: 'success.400', // Unchecked = Software = Green
            borderRadius: 0,
            _checked: {
              bg: 'brand.400', // Checked = Astro Hosted = Purple
            },
          },
        },
      },
    },

    Tag: {
      baseStyle: {
        container: {
          fontWeight: 'semibold',
        },
      },
      variants: {
        solid: {
          container: {
            bg: 'brand.400',
            color: 'white',
          },
        },
      },
    },

    Tooltip: {
      baseStyle: {
        bg: 'moonshot.700',
        color: 'white',
        borderRadius: 'md',
        px: 3,
        py: 2,
        fontSize: 'sm',
      },
    },

    Link: {
      baseStyle: {
        color: 'brand.400',
        _hover: {
          color: 'brand.500',
          textDecoration: 'underline',
        },
      },
    },

  },

  styles: {
    global: {
      body: {
        bg: 'white',
        color: 'moonshot.700',
        fontSize: { base: 'sm', md: 'md' },
        margin: 0,
        padding: 0,
        overflowX: 'hidden',
      },
      // Custom scrollbar styling
      '::-webkit-scrollbar': {
        width: '8px',
        height: '8px',
      },
      '::-webkit-scrollbar-track': {
        bg: 'gray.100',
      },
      '::-webkit-scrollbar-thumb': {
        bg: 'brand.400',
        borderRadius: 'full',
        _hover: {
          bg: 'brand.500',
        },
      },
      // Starship page layout container
      '.starship-page': {
        p: { base: 3, md: 4, lg: 6 },
        borderRadius: 'sm',
        w: '100%',
        m: 0,
        maxW: '100%',
        boxSizing: 'border-box',
      },
      // Data table responsive styling
      '.data-table': {
        overflowX: 'auto',
        w: '100%',
      },
      // Container reset for Airflow embedding
      '.container:has(.row > .starship-main)': {
        p: '0 !important',
        m: '0 !important',
        w: '100% !important',
        h: '100% !important',
        maxW: '100% !important',
      },
      '.starship-main': {
        flexGrow: 1,
        m: '0 !important',
        p: '0 !important',
        border: 0,
        overflow: 'hidden',
        w: '100%',
      },
    },
  },

  // Responsive breakpoints
  breakpoints: {
    base: '0em', // 0px
    sm: '30em', // 480px
    md: '48em', // 768px
    lg: '62em', // 992px
    xl: '80em', // 1280px
    '2xl': '96em', // 1536px
  },
});

export default theme;
