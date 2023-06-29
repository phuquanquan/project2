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
import { Box, SimpleGrid } from "@chakra-ui/react";
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

export default function Settings() {
  // Chakra Color Mode
  return (
    <Box pt={{ base: "130px", md: "80px", xl: "80px" }}>
      <SimpleGrid
        mb='20px'
        columns={{ sm: 1, md: 2 }}
        spacing={{ base: "20px", xl: "20px" }}>
        <FrequentHosts
          columnsData={columnsFrequentHosts}
          tableData={tableFrequentHosts}
        />
        <TopEndpoints columnsData={columnsTopEndpoints} tableData={tableDataCheck} />
        <BadEndpointsTop
          columnsData={columnsBadEndpointsTop}
          tableData={tableBadEndpointsTop}
        />
        <ErrHostsTop
            columnsData={columnsErrHostsTop}
            tableData={tableErrHostsTop}
        />
        <TopTenErrURLs
            columnsData={columnsTopTenErrURLs}
            tableData={tableDataColumns}
        />
      </SimpleGrid>
    </Box>
  );
}
