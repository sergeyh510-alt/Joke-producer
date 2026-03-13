package com.example.jokekafkaproducer.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaSendResultDto {
    private boolean success;
    private String message;
    private Long jokeId;
    private String jokeSetup;
    private String jokePunchline;
    private String topic;
    private Integer partition;
    private Long offset;
    private LocalDateTime timestamp;
    private Long sendDurationMs;
}