import axios from "axios";
import queryString from 'query-string'

// axios instance for making requests
const axiosInstance = axios.create({
    baseURL: "http://192.168.64.144:8080/api/",
    headers: {
        'content-type': 'application/json'
    },
    paramsSerializer: params => queryString.stringify(params),
});

// request interceptor for adding token
axiosInstance.interceptors.request.use((config) => {
    // add token to request headers
    config.headers["Authorization"] = localStorage.getItem("token");
    return config;
});

axiosInstance.interceptors.response.use((reponse) => {
    if (reponse && reponse.data) {
        return reponse.data;
    }
    return reponse;
}, (error) => {
    throw error;
});

export default axiosInstance;