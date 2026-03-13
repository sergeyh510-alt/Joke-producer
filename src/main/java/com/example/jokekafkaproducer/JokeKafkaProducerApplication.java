package com.example.jokekafkaproducer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class JokeKafkaProducerApplication {
    public static void main(String[] args) {
        SpringApplication.run(JokeKafkaProducerApplication.class, args);
    }
}