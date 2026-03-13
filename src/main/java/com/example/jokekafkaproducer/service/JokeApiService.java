package com.example.jokekafkaproducer.service;

import com.example.jokekafkaproducer.model.Joke;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

@Service
@Slf4j
public class JokeApiService {
    private final WebClient webClient;

    public JokeApiService() {
        this.webClient = WebClient.builder()
                .baseUrl("https://official-joke-api.appspot.com")
                .build();
    }

    public Mono<Joke> fetchRandomJoke() {
        log.info("Запрос к Joke API...");

        return webClient.get()
                .uri("/random_joke")
                .retrieve()
                .onStatus(HttpStatusCode::is4xxClientError, response -> {
                    log.error("Клиентская ошибка при запросе к API: {}", response.statusCode());
                    return Mono.error(new RuntimeException("API вернул ошибку: " + response.statusCode()));
                })
                .onStatus(HttpStatusCode::is5xxServerError, response -> {
                    log.error("Серверная ошибка API: {}", response.statusCode());
                    return Mono.error(new RuntimeException("Сервер API недоступен"));
                })
                .bodyToMono(Joke.class)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                        .filter(throwable -> throwable instanceof WebClientResponseException.TooManyRequests))
                .doOnSuccess(joke -> log.info("Получена шутка ID: {}, Setup: {}",
                        joke.getId(), joke.getSetup()))
                .doOnError(error -> {
                    if (error instanceof WebClientResponseException.NotFound) {
                        log.error("API вернул 404 Not Found - возможно изменился endpoint");
                    } else if (error instanceof WebClientResponseException.TooManyRequests) {
                        log.error("Слишком много запросов к API (429)");
                    } else {
                        log.error("Ошибка API: {}", error.getMessage());
                    }
                });
    }
}