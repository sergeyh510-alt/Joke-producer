package com.example.jokekafkaproducer.controller;

import com.example.jokekafkaproducer.dto.KafkaSendResultDto;
import com.example.jokekafkaproducer.service.JokeApiService;
import com.example.jokekafkaproducer.service.JokeKafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/joke")
@RequiredArgsConstructor
@Slf4j
public class JokeController {

    private final JokeApiService jokeApiService;
    private final JokeKafkaProducerService jokeKafkaProducerService;

    @PostMapping("/fetch-and-send")
    public Mono<ResponseEntity<KafkaSendResultDto>> fetchJokeAndSendToKafka() {
        log.info("Запрос на получение и отправку шутки");

        return jokeApiService.fetchRandomJoke()
                .flatMap(joke -> Mono.fromFuture(jokeKafkaProducerService.sendJokeWithAck(joke)))
                .map(result -> ResponseEntity.ok(result))
                .switchIfEmpty(Mono.just(ResponseEntity.notFound().build()))
                .onErrorResume(e -> {
                    log.error("Ошибка: {}", e.getMessage());
                    return Mono.just(ResponseEntity.internalServerError().build());
                });
    }
}