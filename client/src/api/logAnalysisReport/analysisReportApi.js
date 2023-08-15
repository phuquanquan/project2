const { default: axiosInstance } = require("api/customAxiosConfig/CustomAxiosConfig");

const analysisReportApi = {
    getAll: (params) => {
        const url = '/tables/log_analysis:log_analysis_report/head';
        return axiosInstance.get(url, { params });
    },

    get: (rowKey) => {
        const url = `/tables/log_analysis:log_analysis_report/row/${rowKey}`;
        return axiosInstance.get(url);
    },
}

export default analysisReportApi;