import React from "react";

// Chakra imports
import { Flex } from "@chakra-ui/react";

// Custom components
import { HSeparator } from "components/separator/Separator";
import MiniCalendar from "../../calendar/MiniCalendar";

export function SidebarBrand() {

  return (
      <Flex align='center' direction='column'>
        <MiniCalendar h='100%' minW='100%' selectRange={false} />
        <HSeparator mb='20px' />
      </Flex>
  );
}

export default SidebarBrand;
