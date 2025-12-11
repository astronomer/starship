import { useNavigation } from 'react-router-dom';
import { Center, Spinner } from '@chakra-ui/react';
import React from 'react';

export default function AppLoading() {
  const navigation = useNavigation();
  return navigation.state === 'loading' ? (
    <Center>
      <Spinner thickness="6px" speed="0.5s" emptyColor="gray.200" color="brand.400" size="xl" />
    </Center>
  ) : null;
}
