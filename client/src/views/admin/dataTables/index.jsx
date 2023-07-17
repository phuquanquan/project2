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
import {Box, Icon, SimpleGrid, useColorModeValue} from "@chakra-ui/react";
import FrequentHosts from "./components/FrequentHosts";
import TopEndpoints from "./components/TopEndpoints";
import TopTenErrURLs from "./components/TopTenErrURLs";
import BadEndpointsTop from "views/admin/dataTables/components/BadEndpointsTop";
import ErrHostsTop from "views/admin/dataTables/components/ErrHostsTop";

import {
  columnsErrHostsTop,
  columnsFrequentHosts,
  columnsTopEndpoints,
  columnsTopTenErrURLs,
  columnsBadEndpointsTop,
} from "views/admin/dataTables/variables/columnsData";
import tableFrequentHosts from "views/admin/dataTables/variables/tableFrequentHosts.json";
import tableDataCheck from "views/admin/dataTables/variables/tableTopEndpoints.json";
import tableDataColumns from "views/admin/dataTables/variables/tableTopTenErrURLs.json";
import tableBadEndpointsTop from "views/admin/dataTables/variables/tableBadEndpointsTop.json";
import tableErrHostsTop from "views/admin/dataTables/variables/tableErrHostsTop.json";
import React from "react";
import MiniStatistics from "../../../components/card/MiniStatistics";
import IconBox from "../../../components/icons/IconBox";
import {MdBarChart} from "react-icons/md";
import DailyTraffic from "../default/components/DailyTraffic";
import PieCard from "../default/components/PieCard";
import TotalSpent from "../default/components/TotalSpent";

export default function Settings() {
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
            name='Tổng số truy cập thất bại'
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
      <SimpleGrid
        mb='20px'
        columns={{ sm: 1, md: 2 }}
        spacing={{ base: "20px", xl: "20px" }}>
        <SimpleGrid row={{ base: 1, md: 2, xl: 2 }} gap='20px'>
          <TotalSpent />
          <DailyTraffic />
        </SimpleGrid>
        <SimpleGrid row={{ base: 1, md: 2, xl: 2 }} gap='20px'>
          <FrequentHosts
              columnsData={columnsFrequentHosts}
              tableData={tableFrequentHosts}
          />
        </SimpleGrid>
      </SimpleGrid>
      <SimpleGrid
          mb='20px'
          columns={{ sm: 1, md: 2 }}
          spacing={{ base: "20px", xl: "20px" }}>
        <TopEndpoints columnsData={columnsTopEndpoints} tableData={tableDataCheck} />
        <TopTenErrURLs
            columnsData={columnsTopTenErrURLs}
            tableData={tableDataColumns}
        />
        <ErrHostsTop
            columnsData={columnsErrHostsTop}
            tableData={tableErrHostsTop}
        />
        <BadEndpointsTop
            columnsData={columnsBadEndpointsTop}
            tableData={tableBadEndpointsTop}
        />
      </SimpleGrid>
    </Box>
  );
}
