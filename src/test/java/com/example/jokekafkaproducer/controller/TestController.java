package com.example.jokekafkaproducer.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/test")
public class TestController {

    @GetMapping("/simple")
    public ResponseEntity<String> simple() {
        return ResponseEntity.ok("Тестовый контроллер работает");
    }

    @GetMapping("/diagnostic")
    public ResponseEntity<String> diagnostic() {
        return ResponseEntity.ok("Диагностический эндпоинт работает");
    }
}