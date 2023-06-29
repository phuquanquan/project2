package com.example.server.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class ApiController {

    @GetMapping("/data")
    public String getData() {
        // Xử lý yêu cầu từ client và trả về dữ liệu
        return "Dữ liệu từ phía server";
    }

    // Các phương thức xử lý yêu cầu khác
}
