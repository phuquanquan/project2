import {
  Flex,
  Table,
  Progress,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useColorModeValue,
} from "@chakra-ui/react";
import React, { useEffect, useState, useMemo } from "react";
import axios from "axios";
import {
  useGlobalFilter,
  usePagination,
  useSortBy,
  useTable,
} from "react-table";

// Custom components
import Card from "components/card/Card";
import Menu from "components/menu/MainMenu";

export default function ColumnsTable(props) {
  const [tableData, setTableData] = useState([]);
  const [columnsData, setColumnsData] = useState([]);
  const [selectedDate, setSelectedDate] = useState(new Date()); // State để lưu giá trị ngày

  useEffect(() => {
    const formattedDate = selectedDate.toISOString().split("T")[0]; // Chuyển đổi ngày sang định dạng "YYYY-MM-DD"
    // const apiUrl = `http://192.168.64.144:8080/api/tables/log_analysis:log_analysis_report/row/date=${formattedDate}`;
    const apiUrl = `http://192.168.64.144:8080/api/tables/log_analysis:log_analysis_report/row/date=1995-07-01`;
    axios.get(apiUrl)
        .then(response => {
          // Xử lý dữ liệu từ response.data
          setTableData(response.data.columnFamilies[0].columnValues);
          setColumnsData([
            {
              Header: "BAD ENDPOINT",
              accessor: "column",
            },
            {
              Header: "TIMES",
              accessor: "value",
            },
            // ... thêm các cột khác tương tự
          ]);
        })
        .catch(error => {
          console.error("Error fetching data:", error);
        });
  }, [selectedDate]); // Sử dụng selectedDate trong dependency array để gọi API khi ngày thay đổi

  const tableInstance = useTable(
    {
      columns: columnsData,
      data: tableData,
    },
    useGlobalFilter,
    useSortBy,
    usePagination
  );

  const {
    getTableProps,
    getTableBodyProps,
    headerGroups,
    page,
    prepareRow,
    initialState,
  } = tableInstance;
  initialState.pageSize = 10;

  const textColor = useColorModeValue("secondaryGray.900", "white");
  const borderColor = useColorModeValue("gray.200", "whiteAlpha.100");
  return (
    <Card
      direction='column'
      w='100%'
      px='0px'
      overflowX={{ sm: "scroll", lg: "hidden" }}>
      <Flex px='25px' justify='space-between' mb='20px' align='center'>
        <Text
          color={textColor}
          fontSize='22px'
          fontWeight='700'
          lineHeight='100%'>
          Listing the Top Ten 404 Response Code Endpoints
        </Text>
        <Menu />
      </Flex>
      <Table {...getTableProps()} variant='simple' color='gray.500' mb='24px'>
        <Thead>
          {headerGroups.map((headerGroup, index) => (
            <Tr {...headerGroup.getHeaderGroupProps()} key={index}>
              {headerGroup.headers.map((column, index) => (
                <Th
                  {...column.getHeaderProps(column.getSortByToggleProps())}
                  pe='10px'
                  key={index}
                  borderColor={borderColor}>
                  <Flex
                    justify='space-between'
                    align='center'
                    fontSize={{ sm: "10px", lg: "12px" }}
                    color='gray.400'>
                    {column.render("Header")}
                  </Flex>
                </Th>
              ))}
            </Tr>
          ))}
        </Thead>
        <Tbody {...getTableBodyProps()}>
          {page.map((row, index) => {
            prepareRow(row);
            return (
              <Tr {...row.getRowProps()} key={index}>
                {row.cells.map((cell, index) => {
                  let data = "";
                  if (cell.column.Header === "BAD ENDPOINT") {
                    data = (
                        <Text color={textColor} fontSize='sm' fontWeight='700'>
                          {cell.value}
                        </Text>
                    );
                  } else if (cell.column.Header === "TIMES") {
                    data = (
                        <Flex align='center'>
                          <Text
                              me='10px'
                              color={textColor}
                              fontSize='sm'
                              fontWeight='700'>
                            {cell.value}
                          </Text>
                        </Flex>
                    );
                  } else if (cell.column.Header === "LAST TIME") {
                    data = (
                        <Text color={textColor} fontSize='sm' fontWeight='700'>
                          {cell.value}
                        </Text>
                    );
                  } else if (cell.column.Header === "PERCENT OF TOTAL") {
                    data = (
                        <Flex align='center'>
                          <Text
                              me='10px'
                              color={textColor}
                              fontSize='sm'
                              fontWeight='700'>
                            {cell.value}%
                          </Text>
                          <Progress
                              variant='table'
                              colorScheme='brandScheme'
                              h='8px'
                              w='63px'
                              value={cell.value}
                          />
                        </Flex>
                    );
                  }
                  return (
                    <Td
                      {...cell.getCellProps()}
                      key={index}
                      fontSize={{ sm: "14px" }}
                      minW={{ sm: "150px", md: "200px", lg: "auto" }}
                      borderColor='transparent'>
                      {data}
                    </Td>
                  );
                })}
              </Tr>
            );
          })}
        </Tbody>
      </Table>
    </Card>
  );
}
