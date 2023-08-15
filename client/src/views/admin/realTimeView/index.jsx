// Chakra imports
import {
  Box,
  Icon,
  SimpleGrid,
  useColorModeValue,
} from "@chakra-ui/react";

import MiniStatistics from "components/card/MiniStatistics";
import IconBox from "components/icons/IconBox";
import React, { useState, useEffect } from "react";
import {
  MdBarChart,
} from "react-icons/md";
import DailyTraffic from "views/admin/components/DailyTraffic";
import PieCard from "views/admin/components/PieCard";
import TotalSpent from "views/admin/components/TotalSpent";
import {columnsTopEndpoints} from "../variables/columnsData";
import TopEndpoints from "../components/TopEndpoints";
import analysisRealtimeApi from "api/logAnalysisRealtime/analysisRealtimeApi";

export default function UserReports() {
  // Chakra Color Mode
  const brandColor = useColorModeValue("brand.500", "white");
  const boxBg = useColorModeValue("secondaryGray.300", "whiteAlpha.100");
  const [logAnaLysisRealtime, setLogAnaLysisRealtime] = useState(null);

  const fetchAndUpdateData = async () => {
    const currentDate = new Date();
    const year = currentDate.getFullYear();
    const month = (currentDate.getMonth() + 1).toString().padStart(2, "0");
    const day = currentDate.getDate().toString().padStart(2, "0");
    const rowKey = `date=${year}-${month}-${day}`;

    try {
      const response = await analysisRealtimeApi.get("date=2023-08-14");
      console.log('Fetch analysis report successfully: ', response);

      setLogAnaLysisRealtime(response?.columnFamilies[0]?.columnValues);
    } catch (error) {
      console.log('Failed to fetch analysis report: ', error);
    }
  };

  useEffect(() => {
    fetchAndUpdateData(); // Cập nhật lần đầu khi component được mount

    const intervalId = setInterval(() => {
      fetchAndUpdateData(); // Cập nhật sau mỗi 1 phút
    }, 60000); // 60000 milliseconds = 1 phút

    // Clear interval khi component bị unmount
    return () => clearInterval(intervalId);
  }, []);

  console.log(logAnaLysisRealtime);

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
          {/* <TopEndpoints columnsData={columnsTopEndpoints} tableData={tableDataCheck} /> */}
          {/* <BadEndpointsTop columnsData={columnsBadEndpointsTop} tableData={tableBadEndpointsTop} /> */}
      </SimpleGrid>
    </Box>
  );
}
