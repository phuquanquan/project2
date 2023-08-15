import React, { useState, useEffect, useCallback  } from "react";
import Calendar from "react-calendar";
import "react-calendar/dist/Calendar.css";
import "assets/css/MiniCalendar.css";
import { Text, Icon } from "@chakra-ui/react";
import { MdChevronLeft, MdChevronRight } from "react-icons/md";
import Card from "components/card/Card.js";
import { useDate } from "../sidebar/components/DateContext";

export default function MiniCalendar(props) {
    const { selectRange, ...rest } = props;
    const { selectedDate, handleDateChange } = useDate();

    // Đặt ngày cố định là 1995-08-30
    const fixedDate = new Date("1995-08-30");

    // Sử dụng ngày cố định làm giá trị ban đầu
    const [value, onChange] = useState(selectedDate || fixedDate);

    const handleCalendarChange = useCallback(
        (date) => {
        onChange(date);
        handleDateChange(date); // Cập nhật selectedDate trong Context
        },
        [handleDateChange]
    );

    useEffect(() => {
        handleCalendarChange(value);
    }, [handleCalendarChange, value]);
    
    return (
        <Card
            align='center'
            direction='column'
            w='100%'
            maxW='max-content'
            p='20px 15px'
            h='max-content'
            {...rest}>
            <Calendar
                onChange={onChange}
                value={value}
                selectRange={selectRange}
                view={"month"}
                tileContent={<Text color='brand.500'></Text>}
                prevLabel={<Icon as={MdChevronLeft} w='24px' h='24px' mt='4px' />}
                nextLabel={<Icon as={MdChevronRight} w='24px' h='24px' mt='4px' />}
            />
        </Card>
    );
}
