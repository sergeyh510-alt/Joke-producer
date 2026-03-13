package com.example.jokekafkaproducer.service;

import com.example.jokekafkaproducer.model.Joke;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@Slf4j
@RequiredArgsConstructor
public class ScheduledJokeService {

    private final JokeApiService jokeApiService;
    private final JokeKafkaProducerService jokeKafkaProducerService;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    @Value("${joke.scheduler.fixed-rate:10000}")
    private long fixedRate;

    @Scheduled(fixedRateString = "${joke.scheduler.fixed-rate:10000}")
    public void fetchAndSendJokeAutomatically() {
        if (!isRunning.compareAndSet(false, true)) {
            log.warn("Предыдущая задача ещё выполняется — пропускаем выполнение");
            return;
        }

        jokeApiService.fetchRandomJoke()
                .timeout(Duration.ofSeconds(5))
                .flatMap(joke -> {
                    log.debug("Получена шутка ID: {}", joke.getId());
                    return Mono.fromFuture(jokeKafkaProducerService.sendJokeWithAck(joke));
                })
                .timeout(Duration.ofSeconds(10))
                .retryWhen(Retry.fixedDelay(2, Duration.ofSeconds(2)))
                .doOnSuccess(result -> log.info("Автоматическая отправка успешна. Offset: {}", result.getOffset()))
                .doOnError(error -> log.error("Ошибка при автоматической отправке: {}", error.getMessage()))
                .doFinally(signal -> isRunning.set(false))
                .subscribe();
    }
}
