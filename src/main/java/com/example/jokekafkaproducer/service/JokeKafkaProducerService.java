package com.example.jokekafkaproducer.service;

import com.example.jokekafkaproducer.dto.KafkaSendResultDto;
import com.example.jokekafkaproducer.model.Joke;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class JokeKafkaProducerService {

    //private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String TOPIC = "jokes-topic";
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public CompletableFuture<KafkaSendResultDto> sendJokeWithAck(Joke joke) {
        long startTime = System.currentTimeMillis();
        log.info("📤 Отправка шутки ID {} в Kafka...", joke.getId());

        CompletableFuture<SendResult<String, Object>> future;

        try {
            future = kafkaTemplate.send(TOPIC, String.valueOf(joke.getId()), joke);
        } catch (Exception e) {
            log.error("Ошибка при отправке в Kafka: {}", e.getMessage());
            CompletableFuture<KafkaSendResultDto> errorFuture = new CompletableFuture<>();
            errorFuture.complete(KafkaSendResultDto.builder()
                    .success(false)
                    .message("Ошибка отправки: " + e.getMessage())
                    .jokeId((long) joke.getId())
                    .jokeSetup(joke.getSetup())
                    .jokePunchline(joke.getPunchline())
                    .timestamp(LocalDateTime.now())
                    .sendDurationMs(System.currentTimeMillis() - startTime)
                    .build());
            return errorFuture;
        }

        return future.handle((result, ex) -> {
            long duration = System.currentTimeMillis() - startTime;

            if (ex == null) {
                log.info("ПОДТВЕРЖДЕНО: Шутка ID {} записана. Партиция: {}, Оффсет: {}, Время: {} мс",
                        joke.getId(), result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(), duration);

                return KafkaSendResultDto.builder()
                        .success(true)
                        .message("Шутка успешно записана в Kafka (подтверждено брокером)")
                        .jokeId((long) joke.getId())
                        .jokeSetup(joke.getSetup())
                        .jokePunchline(joke.getPunchline())
                        .topic(result.getRecordMetadata().topic())
                        .partition(result.getRecordMetadata().partition())
                        .offset(result.getRecordMetadata().offset())
                        .timestamp(LocalDateTime.now())
                        .sendDurationMs(duration)
                        .build();
            } else {
                log.error("ОШИБКА: Не получено подтверждение для шутки ID {}: {}",
                        joke.getId(), ex.getMessage());

                return KafkaSendResultDto.builder()
                        .success(false)
                        .message("Ошибка записи в Kafka: " + ex.getMessage())
                        .jokeId((long) joke.getId())
                        .jokeSetup(joke.getSetup())
                        .jokePunchline(joke.getPunchline())
                        .timestamp(LocalDateTime.now())
                        .sendDurationMs(duration)
                        .build();
            }
        });
    }
}