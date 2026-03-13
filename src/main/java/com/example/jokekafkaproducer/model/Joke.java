package com.example.jokekafkaproducer.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Joke {
    private String type;
    private String setup;
    private String punchline;
    private int id;
}