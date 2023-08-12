import React, { useState } from "react";

// Chakra imports
import { Flex } from "@chakra-ui/react";

// Custom components
import { HSeparator } from "components/separator/Separator";
import MiniCalendar from "../../calendar/MiniCalendar";

export function SidebarBrand() {
  const [selectedDate, setSelectedDate] = useState(null);

  // Hàm xử lý sự kiện khi ngày thay đổi
  const handleDateChange = (date) => {
      setSelectedDate(date);
      console.log("Selected date:", date);
  };

  return (
    <Flex align='center' direction='column'>
      <MiniCalendar onDateChange={handleDateChange} h='100%' minW='100%' selectRange={false} />
      <HSeparator mb='20px' />
      {/* Hiển thị giá trị ngày */}
      <p>Selected date: {selectedDate && selectedDate.toDateString()}</p>
    </Flex>
  );
}

export default SidebarBrand;
