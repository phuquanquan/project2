import React from "react";

import { Icon } from "@chakra-ui/react";
import {
  MdBarChart,
  MdHome,
} from "react-icons/md";

// Admin Imports
import RealTimeView from "views/admin/realTimeView";
import BatchView from "views/admin/batchView";

const routes = [
  {
    name: "Real Time View",
    layout: "/admin",
    path: "/real-time-view",
    icon: <Icon as={MdHome} width='20px' height='20px' color='inherit' />,
    component: RealTimeView,
  },
  {
    name: "Batch View",
    layout: "/admin",
    icon: <Icon as={MdBarChart} width='20px' height='20px' color='inherit' />,
    path: "/batch-view",
    component: BatchView,
  },
];

export default routes;
