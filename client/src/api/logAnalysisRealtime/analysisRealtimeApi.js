const { default: axiosInstance } = require("api/customAxiosConfig/CustomAxiosConfig");

const analysisRealtimeApi = {
    getAll: (params) => {
        const url = '/tables/log_analysis:log_analysis_realtime/head';
        return axiosInstance.get(url, { params });
    },

    get: (rowKey) => {
        const url = `/tables/log_analysis:log_analysis_realtime/row/${rowKey}`;
        return axiosInstance.get(url);
    },
}

export default analysisRealtimeApi;
