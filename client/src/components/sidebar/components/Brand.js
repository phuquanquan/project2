import React from "react";

// Chakra imports
import { Flex, useColorModeValue } from "@chakra-ui/react";

// Custom components
import { HorizonLogo } from "components/icons/Icons";
import { HSeparator } from "components/separator/Separator";
import MiniCalendar from "../../calendar/MiniCalendar";

export function SidebarBrand() {
  //   Chakra color mode
  let logoColor = useColorModeValue("navy.700", "white");

  return (
    <Flex align='center' direction='column'>
      <MiniCalendar h='100%' minW='100%' selectRange={false} />
      <HSeparator mb='20px' />
    </Flex>
  );
}

export default SidebarBrand;
