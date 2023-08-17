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
import {
  columnsTopEndpoints,
  columnsTopErrURLs,
} from 'views/admin/variables/columnsData';

import DailyTraffic from "views/admin/components/DailyTraffic";
import PieCard from "views/admin/components/PieCard";
import TotalSpent from "views/admin/components/TotalSpent";
import TopEndpoints from "../components/TopEndpoints";
import TopErrURLs from "../components/TopErrorEndpoint";
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
      const response = await analysisRealtimeApi.get(rowKey);
      console.log('Fetch analysis realtime successfully: ', response);

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
          name='Total number of visits'
          value={(logAnaLysisRealtime[7]?.value)}
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
          name='Total number of successful'
          value={(logAnaLysisRealtime[8]?.value)}
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
            name='Number of people online'
            value="10"
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
          name='Content Size Avg'
          value={Math.round((logAnaLysisRealtime[5]?.value)/(logAnaLysisRealtime[8]?.value))}
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
          name='Number of Unique Hosts'
          value={(logAnaLysisRealtime[9]?.value)}
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
          name='Error code number'
          value={(logAnaLysisRealtime[0]?.value)}
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
          <TopEndpoints columnsData={columnsTopEndpoints} tableData={JSON.parse(logAnaLysisRealtime[4].value)} />
          <TopErrURLs columnsData={columnsTopErrURLs} tableData={JSON.parse(logAnaLysisRealtime[1].value)} />
      </SimpleGrid>
    </Box>
  );
}
