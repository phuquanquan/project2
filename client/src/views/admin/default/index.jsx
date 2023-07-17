// Chakra imports
import {
  Avatar,
  Box,
  Flex,
  FormLabel,
  Icon,
  Select,
  SimpleGrid,
  useColorModeValue,
} from "@chakra-ui/react";
// Assets
import Usa from "assets/img/dashboards/usa.png";
// Custom components
import MiniCalendar from "components/calendar/MiniCalendar";
import MiniStatistics from "components/card/MiniStatistics";
import IconBox from "components/icons/IconBox";
import React from "react";
import {
  MdBarChart,
} from "react-icons/md";
import DailyTraffic from "views/admin/default/components/DailyTraffic";
import PieCard from "views/admin/default/components/PieCard";
import TotalSpent from "views/admin/default/components/TotalSpent";
import WeeklyRevenue from "views/admin/default/components/WeeklyRevenue";
import {columnsBadEndpointsTop, columnsTopEndpoints} from "../dataTables/variables/columnsData";
import tableDataCheck from "../dataTables/variables/tableTopEndpoints.json";
import TopEndpoints from "../dataTables/components/TopEndpoints";
import tableBadEndpointsTop from "../dataTables/variables/tableBadEndpointsTop.json";
import BadEndpointsTop from "../dataTables/components/BadEndpointsTop";

export default function UserReports() {
  // Chakra Color Mode
  const brandColor = useColorModeValue("brand.500", "white");
  const boxBg = useColorModeValue("secondaryGray.300", "whiteAlpha.100");
  return (
    <Box pt={{ base: "130px", md: "80px", xl: "80px" }}>
      <SimpleGrid
        columns={{ base: 1, md: 2, lg: 3, "2xl": 6 }}
        gap='20px'
        mb='20px'>
        <MiniStatistics
          startContent={
            <IconBox
              w='56px'
              h='56px'
              bg={boxBg}
              icon={
                <Icon w='32px' h='32px' as={MdBarChart} color={brandColor} />
              }
            />
          }
          name='Tổng số truy cập'
          value='232323232'
        />
        <MiniStatistics
            startContent={
                <IconBox
                    w='56px'
                    h='56px'
                    bg={boxBg}
                    icon={
                        <Icon w='32px' h='32px' as={MdBarChart} color={brandColor} />
                    }
                />
            }
          name='Tổng số truy cập thành công'
          value='222222222'
        />
        <MiniStatistics
            startContent={
                <IconBox
                    w='56px'
                    h='56px'
                    bg={boxBg}
                    icon={
                        <Icon w='32px' h='32px' as={MdBarChart} color={brandColor} />
                    }
                />
            }
            name='Số người đang trực tuyến'
            value='1000'
        />
        <MiniStatistics
            startContent={
                <IconBox
                    w='56px'
                    h='56px'
                    bg={boxBg}
                    icon={
                        <Icon w='32px' h='32px' as={MdBarChart} color={brandColor} />
                    }
                />
            }
          name='Kích thước nội dung trung bình'
          value='17531'
        />
        <MiniStatistics
            startContent={
                <IconBox
                    w='56px'
                    h='56px'
                    bg={boxBg}
                    icon={
                        <Icon w='32px' h='32px' as={MdBarChart} color={brandColor} />
                    }
                />
            }
          name='Số lượng máy chủ duy nhất'
          value='54507'
        />
        <MiniStatistics
            startContent={
                <IconBox
                    w='56px'
                    h='56px'
                    bg={boxBg}
                    icon={
                        <Icon w='32px' h='32px' as={MdBarChart} color={brandColor} />
                    }
                />
            }
          name='Tổng số mã lỗi 404'
          value='6185'
        />
      </SimpleGrid>

      <SimpleGrid columns={{ base: 1, md: 2, xl: 2 }} gap='20px' mb='20px'>
        <TotalSpent />
          <SimpleGrid columns={{ base: 1, md: 2, xl: 2 }} gap='20px'>
              <DailyTraffic />
              <PieCard />
          </SimpleGrid>
        {/*<WeeklyRevenue />*/}
      </SimpleGrid>
      <SimpleGrid columns={{ base: 1, md: 1, xl: 2 }} gap='20px' mb='20px'>
          <TopEndpoints columnsData={columnsTopEndpoints} tableData={tableDataCheck} />
          <BadEndpointsTop columnsData={columnsBadEndpointsTop} tableData={tableBadEndpointsTop} />
      </SimpleGrid>
    </Box>
  );
}
