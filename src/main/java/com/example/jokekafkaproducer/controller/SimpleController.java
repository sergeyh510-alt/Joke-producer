package com.example.jokekafkaproducer.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SimpleController {

    @GetMapping("/simple-ping")
    public ResponseEntity<String> simplePing() {
        return ResponseEntity.ok("Простейший тест работает!");
    }
}