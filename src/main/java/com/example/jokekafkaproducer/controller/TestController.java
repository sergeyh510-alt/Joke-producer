package com.example.jokekafkaproducer.controller;

import com.example.jokekafkaproducer.dto.KafkaSendResultDto;
import com.example.jokekafkaproducer.model.Joke;
import com.example.jokekafkaproducer.service.JokeApiService;
import com.example.jokekafkaproducer.service.JokeKafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/test")
@RequiredArgsConstructor
@Slf4j
public class TestController {

    private final JokeApiService jokeApiService;
    private final JokeKafkaProducerService jokeKafkaProducerService;

    /**
     * Простой тест - уже работает
     */
    @GetMapping("/simple")
    public ResponseEntity<String> simple() {
        return ResponseEntity.ok("Тестовый контроллер работает");
    }

    /**
     * Простая диагностика - уже работает
     */
    @GetMapping("/diagnostic")
    public ResponseEntity<String> diagnostic() {
        return ResponseEntity.ok("Диагностический эндпоинт работает");
    }

    /**
     * Полная диагностика с проверкой API и Kafka
     */
    @GetMapping("/full-diagnostic")
    public Mono<ResponseEntity<Map<String, Object>>> fullDiagnostic() {
        log.info("Запуск полной диагностики...");

        Map<String, Object> result = new HashMap<>();
        result.put("timestamp", LocalDateTime.now().toString());
        result.put("testController", "Работает");

        return jokeApiService.fetchRandomJoke()
                .flatMap(joke -> {
                    result.put("api", "Доступен");
                    result.put("joke", Map.of(
                            "id", joke.getId(),
                            "setup", joke.getSetup(),
                            "type", joke.getType()
                    ));

                    return Mono.fromFuture(jokeKafkaProducerService.sendJokeWithAck(joke))
                            .map(kafkaResult -> {
                                if (kafkaResult.isSuccess()) {
                                    result.put("kafka", "Доступна");
                                    result.put("kafkaDetails", Map.of(
                                            "topic", kafkaResult.getTopic(),
                                            "partition", kafkaResult.getPartition(),
                                            "offset", kafkaResult.getOffset(),
                                            "duration", kafkaResult.getSendDurationMs()
                                    ));
                                } else {
                                    result.put("kafka", "Ошибка");
                                    result.put("kafkaError", kafkaResult.getMessage());
                                }
                                return ResponseEntity.ok(result);
                            });
                })
                .onErrorResume(error -> {
                    result.put("api", "Недоступен");
                    result.put("apiError", error.getMessage());
                    result.put("kafka", "⚠Не проверялся");
                    return Mono.just(ResponseEntity.ok(result));
                });
    }

    /**
     * Проверка только API
     */
    @GetMapping("/check-api")
    public Mono<ResponseEntity<Map<String, Object>>> checkApi() {
        return jokeApiService.fetchRandomJoke()
                .map(joke -> {
                    Map<String, Object> result = new HashMap<>();
                    result.put("status", "API работает");
                    result.put("jokeId", joke.getId());
                    result.put("setup", joke.getSetup());
                    result.put("punchline", joke.getPunchline());
                    return ResponseEntity.ok(result);
                })
                .onErrorResume(error -> {
                    Map<String, Object> result = new HashMap<>();
                    result.put("status", "API не работает");
                    result.put("error", error.getMessage());
                    return Mono.just(ResponseEntity.status(500).body(result));
                });
    }

    /**
     * Проверка только Kafka (с тестовой шуткой)
     */
    @PostMapping("/check-kafka")
    public Mono<ResponseEntity<Map<String, Object>>> checkKafka() {
        Joke testJoke = new Joke();
        testJoke.setId(999);
        testJoke.setType("test");
        testJoke.setSetup("Тестовая диагностическая шутка");
        testJoke.setPunchline("Проверка связи с Kafka");

        return Mono.fromFuture(jokeKafkaProducerService.sendJokeWithAck(testJoke))
                .map(result -> {
                    Map<String, Object> response = new HashMap<>();
                    if (result.isSuccess()) {
                        response.put("status", "Kafka работает");
                        response.put("topic", result.getTopic());
                        response.put("partition", result.getPartition());
                        response.put("offset", result.getOffset());
                        response.put("duration", result.getSendDurationMs());
                    } else {
                        response.put("status", "Kafka ошибка");
                        response.put("error", result.getMessage());
                    }
                    return ResponseEntity.ok(response);
                });
    }

    /**
     * Полный цикл: API → Kafka
     */
    @PostMapping("/full-cycle")
    public Mono<ResponseEntity<Map<String, Object>>> fullCycle() {
        log.info("Запуск полного цикла API → Kafka...");

        return jokeApiService.fetchRandomJoke()
                .flatMap(joke -> {
                    log.info("Получена шутка ID: {}", joke.getId());

                    return Mono.fromFuture(jokeKafkaProducerService.sendJokeWithAck(joke))
                            .map(kafkaResult -> {
                                Map<String, Object> response = new HashMap<>();
                                response.put("success", kafkaResult.isSuccess());
                                response.put("message", kafkaResult.getMessage());
                                response.put("joke", Map.of(
                                        "id", joke.getId(),
                                        "setup", joke.getSetup(),
                                        "punchline", joke.getPunchline()
                                ));

                                if (kafkaResult.isSuccess()) {
                                    response.put("kafka", Map.of(
                                            "topic", kafkaResult.getTopic(),
                                            "partition", kafkaResult.getPartition(),
                                            "offset", kafkaResult.getOffset()
                                    ));
                                }

                                response.put("timestamp", LocalDateTime.now());
                                return ResponseEntity.ok(response);
                            });
                });
    }
    @GetMapping("/kafka-test-simple")
    public ResponseEntity<String> kafkaTestSimple() {
        try {
            // Просто проверяем, создается ли producer
            jokeKafkaProducerService.toString(); // любая операция
            return ResponseEntity.ok("Kafka producer создан успешно");
        } catch (Exception e) {
            return ResponseEntity.status(500)
                    .body("Ошибка: " + e.getMessage());
        }
    }
}