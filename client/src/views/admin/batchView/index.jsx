/*!
  _   _  ___  ____  ___ ________  _   _   _   _ ___   
 | | | |/ _ \|  _ \|_ _|__  / _ \| \ | | | | | |_ _| 
 | |_| | | | | |_) || |  / / | | |  \| | | | | || | 
 |  _  | |_| |  _ < | | / /| |_| | |\  | | |_| || |
 |_| |_|\___/|_| \_\___/____\___/|_| \_|  \___/|___|
                                                                                                                                                                                                                                                                                                                                       
=========================================================
* Horizon UI - v1.1.0
=========================================================

* Product Page: https://www.horizon-ui.com/
* Copyright 2022 Horizon UI (https://www.horizon-ui.com/)

* Designed and Coded by Simmmple

=========================================================

* The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

*/

// Chakra imports
import { Box, Icon, SimpleGrid, useColorModeValue } from '@chakra-ui/react';
import FrequentHosts from '../components/FrequentHosts';
import TopEndpoints from '../components/TopEndpoints';
import TopErrURLs from '../components/TopErrorEndpoint';
import ErrHostsTop from 'views/admin/components/ErrorHostsTop';

import {
  columnsErrHostsTop,
  columnsFrequentHosts,
  columnsTopEndpoints,
  columnsTopErrURLs,
} from 'views/admin/variables/columnsData';
import React, { useEffect, useState } from 'react';
import MiniStatistics from '../../../components/card/MiniStatistics';
import IconBox from '../../../components/icons/IconBox';
import { MdBarChart } from 'react-icons/md';
import DailyTraffic from '../components/DailyTraffic';
import TotalSpent from '../components/TotalSpent';
import analysisReportApi from 'api/logAnalysisReport/analysisReportApi';
import PieCard from "views/admin/components/PieCard";
import { useDate } from "../../../components/sidebar/components/DateContext";

export default function Settings() {
  // Chakra Color Mode
  const brandColor = useColorModeValue('brand.500', 'white');
  const boxBg = useColorModeValue('secondaryGray.300', 'whiteAlpha.100');
  const [logAnaLysisReport, setLogAnaLysisReport] = useState(null);
  const { selectedDate } = useDate();

  useEffect(() => {
    if (selectedDate) {
      const year = selectedDate.getFullYear();
      const month = (selectedDate.getMonth() + 1).toString().padStart(2, "0");
      const day = selectedDate.getDate().toString().padStart(2, "0");
      
      const rowKey = `date=${year}-${month}-${day}`;

      const fetchAnalysisReport = async () => {
        try {
          const response = await analysisReportApi.get(rowKey);
          console.log('Fetch analysis report successfully: ', response);

          setLogAnaLysisReport(response?.columnFamilies[0]?.columnValues);
        } catch (error) {
          console.log('Failed to fetch analysis report: ', error);
        }
      }

      fetchAnalysisReport();
    } else {
      console.log('No date selected.');
    }
  }, [selectedDate]);

  console.log(logAnaLysisReport);

  return (
    <>
      {logAnaLysisReport !== null ?
        <Box pt={{ base: '130px', md: '80px', xl: '80px' }}>
          <SimpleGrid columns={{ base: 1, md: 2, lg: 3, '2xl': 6 }} gap='20px' mb='20px'>
            <MiniStatistics
              startContent={
                <IconBox
                  w='56px'
                  h='56px'
                  bg={boxBg}
                  icon={<Icon w='32px' h='32px' as={MdBarChart} color={brandColor} />}
                />
              }
              name='Total number of visits'
              value={(logAnaLysisReport[10]?.value)}
            />
            <MiniStatistics
              startContent={
                <IconBox
                  w='56px'
                  h='56px'
                  bg={boxBg}
                  icon={<Icon w='32px' h='32px' as={MdBarChart} color={brandColor} />}
                />
              }
              name='Total number of successful'
              value={(logAnaLysisReport[11]?.value)}
            />
            <MiniStatistics
              startContent={
                <IconBox
                  w='56px'
                  h='56px'
                  bg={boxBg}
                  icon={<Icon w='32px' h='32px' as={MdBarChart} color={brandColor} />}
                />
              }
              name='Total failed access'
              value={(logAnaLysisReport[9]?.value)}
            />
            <MiniStatistics
              startContent={
                <IconBox
                  w='56px'
                  h='56px'
                  bg={boxBg}
                  icon={<Icon w='32px' h='32px' as={MdBarChart} color={brandColor} />}
                />
              }
              name='Content Size Avg'
              value={(Math.round(logAnaLysisReport[0]?.value))}
            />
            <MiniStatistics
              startContent={
                <IconBox
                  w='56px'
                  h='56px'
                  bg={boxBg}
                  icon={<Icon w='32px' h='32px' as={MdBarChart} color={brandColor} />}
                />
              }
              name='Number of Unique Hosts'
              value={(logAnaLysisReport[12]?.value)}
            />
            <MiniStatistics
              startContent={
                <IconBox
                  w='56px'
                  h='56px'
                  bg={boxBg}
                  icon={<Icon w='32px' h='32px' as={MdBarChart} color={brandColor} />}
                />
              }
              name='Error code number'
              value={(logAnaLysisReport[1]?.value)}
            />
          </SimpleGrid>
          <SimpleGrid mb='20px' columns={{ sm: 1, md: 2 }} spacing={{ base: '20px', xl: '20px' }}>
            <SimpleGrid row={{ base: 1, md: 2, xl: 2 }} gap='20px'>
              <TotalSpent selectedDate={selectedDate}/>
              <DailyTraffic />
            </SimpleGrid>
            <SimpleGrid row={{ base: 1, md: 2, xl: 2 }} gap='20px'>
              <TotalSpent />
              <PieCard />
            </SimpleGrid>
          </SimpleGrid>
          <SimpleGrid mb='20px' columns={{ sm: 1, md: 2 }} spacing={{ base: '20px', xl: '20px' }}>
            <FrequentHosts columnsData={columnsFrequentHosts} tableData={JSON.parse(logAnaLysisReport[5].value)} />
            <TopEndpoints columnsData={columnsTopEndpoints} tableData={JSON.parse(logAnaLysisReport[7].value)} />
            <TopErrURLs columnsData={columnsTopErrURLs} tableData={JSON.parse(logAnaLysisReport[8].value)} />
            <ErrHostsTop columnsData={columnsErrHostsTop} tableData={JSON.parse(logAnaLysisReport[3].value)} />
          </SimpleGrid>
        </Box>
        : null
      }
    </>
  );
}
