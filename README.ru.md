## 🃏 Kafka Producer Service with API Integration

[![🇬🇧 English](https://img.shields.io/badge/🇬🇧_English-README-blue?style=for-the-badge&logo=markdown&logoColor=white)](./README.md)
[![🇷🇺 Русский](https://img.shields.io/badge/🇷🇺_Русский-README-red?style=for-the-badge&logo=markdown&logoColor=white)](./README.ru.md)

---

[![Java](https://img.shields.io/badge/Java-ED8B00?style=for-the-badge&logo=java&logoColor=white)](https://adoptium.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-6DB33F?style=for-the-badge&logo=springboot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](LICENSE)
[![Version](https://img.shields.io/badge/Version-1.0-blue.svg?style=for-the-badge)](https://github.com/yourname/joke-kafka-producer)
[![Java](https://img.shields.io/badge/Java-19.0.2+-orange.svg?style=for-the-badge)](https://adoptium.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.0.3-brightgreen.svg?style=for-the-badge)](https://spring.io/projects/spring-boot)
[![Status](https://img.shields.io/badge/status-stable-brightgreen.svg?style=for-the-badge)](https://github.com/yourname/joke-kafka-producer)

**Сервис-продюсер для автоматического получения случайных шуток из внешнего API и их отправки в Apache Kafka с подтверждением доставки.**  
`Java` · `Spring Boot` · `Kafka` · `WebFlux` · `Reactor`

|Ключевая особенность|	Описание|
|----------------------------|-------------------------------|
|⚡ Reactive	|Асинхронная обработка на Spring WebFlux + Reactor
|🎯 Атомарность	|Получение шутки → Отправка в Kafka за один цикл
|🔁 Автоматизация	|Планировщик для периодической отправки (каждые 10 сек)
|📊 Диагностика|	Полный набор эндпоинтов для мониторинга
|✅ Подтверждение	|acks=all — гарантия доставки на все реплики
## 📋 Оглавление

  *  [Описание](#Описание)

  *  [Архитектура](#Архитектура)

  *  [Стек технологий](#Стек-технологий)

  *  [Требования](#Требования)

  *  [Установка и запуск](#Установка-и-запуск)

  *  [Конфигурация](#Конфигурация)

  *  [API Endpoints](#API-Endpoints)

  *  [Мониторинг и диагностика](#Мониторинг-и-диагностика)

  *  [Структура проекта](#Структура-проекта)

  *  [Планы по развитию](#Планы-по-развитию)
  *  [Лицензия](#Лицензия)
  *  [Контакты](#Контакты)

## 🎯 
## Описание

### Joke Kafka Producer — это Spring Boot приложение, которое:

 *   Получает случайные шутки из публичного API official-joke-api

 *   Отправляет их в топик Kafka jokes-topic с подтверждением доставки (acks=all)

 *   Работает в двух режимах:

      *  Ручной — по REST-запросу

      *  Автоматический — по расписанию (каждые 10 секунд)

## 🏗 
## Архитектура     
<img width="2452" height="2880" alt="deepseek_mermaid_20260908_38ce5f" src="https://github.com/user-attachments/assets/b8b21495-7dda-44c1-9bee-4a18cd70acc5" />

## Диаграмма последовательности (Sequence Diagram)
<img width="5731" height="3283" alt="deepseek_mermaid_20260908_ccdb23" src="https://github.com/user-attachments/assets/701a0737-2266-4067-b83e-58889ceb5baa" />

## 🛠 
## Стек технологий

|Компонент|	Технология|
|--------------------|--------------------------|
|Язык|	Java 19|
|Фреймворк|	Spring Boot 4.0.3|
|Reactive|	Spring WebFlux + Project Reactor|
|Kafka	|Apache Kafka (Spring Kafka)|
|Сериализация	|Jackson JSON|
|HTTP Client	|WebClient (Reactive)|
|Сборка	|Maven|
|Lombok|	Для сокращения кода|

## 📦 
## Требования

* Java 19 или выше

* Apache Kafka (локально или удалённо)

* Maven 3.8+

* Интернет (для доступа к API шуток)


## 🚀 
## Установка и запуск

### 1. Клонирование репозитория
```bash
git clone https://github.com/yourname/joke-kafka-producer.git
cd joke-kafka-producer
```
### 2. Запуск Kafka (локально)

#### Вариант A: Через Docker Compose
```bach
# docker-compose.yml
version: '3'
services:
  kafka:
    image: apache/kafka:latest
    ports:
      - "9092:9092"
    environment:
      - KAFKA_NODE_ID=1
      - KAFKA_PROCESS_ROLES=broker,controller
      - KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093
      - KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092
      - KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      - KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
      - KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1
      - KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
      - KAFKA_LOG_DIRS=/tmp/kraft-combined-logs
```
### Запуск:
```bach
docker-compose up -d
```
#### Вариант B: Через установленный Kafka
```bach
# Запуск брокера (KRaft mode)
bin/kafka-server-start.sh config/kraft/server.properties
```
### 3. Сборка и запуск приложения
```bach
# Сборка
mvn clean package

# Запуск
mvn spring-boot:run
```
#### Или через JAR:
```bach
java -jar target/joke-kafka-producer-0.0.1-SNAPSHOT.jar
```
## ⚙️ 
## Конфигурация
### application.properties
```bach
#properties
# Приложение
spring.application.name=joke-kafka-producer

# Kafka
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JacksonJsonSerializer

# Топик
joke.kafka.topic=jokes-topic

# Scheduler (мс)
joke.scheduler.fixed-rate=10000

# Логирование
logging.level.com.example.jokekafkaproducer=DEBUG
```
### Настройки Kafka в коде (KafkaConfig.java)
|Параметр	|Значение	|Описание|
|---------|---------|-------------|
|acks	|all|	Подтверждение от всех реплик|
|retries|	3	|Количество попыток при ошибке|
|partitions	|1|	Количество партиций топика|
|replicas	|1|	Количество реплик|

## 🔌 
## API Endpoints
### POST /api/joke/fetch-and-send

#### Получить случайную шутку из API и отправить в Kafka.

### Ответ:
```bach
{
  "success": true,
  "message": "Шутка успешно записана в Kafka (подтверждено брокером)",
  "jokeId": 123,
  "jokeSetup": "Why don't scientists trust atoms?",
  "jokePunchline": "Because they make up everything!",
  "topic": "jokes-topic",
  "partition": 0,
  "offset": 42,
  "timestamp": "2026-09-08T12:34:56",
  "sendDurationMs": 145
}
```
## Диагностические эндпоинты (TestController)

|Метод	|URL	|Описание|
|--------------|------------|-----------------|
GET|	/api/test/simple|	Проверка работы контроллера|
GET|	/api/test/diagnostic	|Простая диагностика|
GET|	/api/test/full-diagnostic|	Полная диагностика (API + Kafka)|
GET|	/api/test/check-api	|Проверка только API|
POST|	/api/test/check-kafka|	Проверка только Kafka (с тестовой шуткой)|
POST|	/api/test/full-cycle	|Полный цикл (API → Kafka)|
GET|	/api/test/kafka-test-simple|	Проверка создания Kafka producer|
GET|	/simple-ping	|Простейший тест|

## 📊 
## Мониторинг и диагностика
### Примеры запросов

#### Проверка API:
```bach
curl -X GET http://localhost:8080/api/test/check-api
```
#### Проверка Kafka:
```bach
curl -X POST http://localhost:8080/api/test/check-kafka
```
#### Полный диагностический отчет:

```bach
curl -X GET http://localhost:8080/api/test/full-diagnostic
```
#### Получение и отправка шутки:
```bach
curl -X POST http://localhost:8080/api/joke/fetch-and-send
```
## 📁
## Структура проекта

```bach
joke-kafka-producer/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/example/jokekafkaproducer/
│   │   │       ├── JokeKafkaProducerApplication.java   # Точка входа
│   │   │       ├── config/
│   │   │       │   └── KafkaConfig.java                # Настройки Kafka
│   │   │       ├── controller/
│   │   │       │   ├── JokeController.java             # Основной REST API
│   │   │       │   ├── TestController.java             # Диагностика
│   │   │       │   └── SimpleController.java           # Простой тест
│   │   │       ├── dto/
│   │   │       │   └── KafkaSendResultDto.java         # DTO ответа
│   │   │       ├── model/
│   │   │       │   └── Joke.java                       # Модель шутки
│   │   │       └── service/
│   │   │           ├── JokeApiService.java             # HTTP клиент к API
│   │   │           ├── JokeKafkaProducerService.java   # Отправка в Kafka
│   │   │           └── ScheduledJokeService.java       # Планировщик
│   │   └── resources/
│   │       ├── application.properties                  # Конфигурация
│   │       ├── static/                                 # Статика
│   │       └── templates/                              # Шаблоны
│   └── test/                                           # Тесты
├── pom.xml                                             # Maven зависимости
├── .gitignore
└── README.md
```

## 🗺
## Планы по развитию
□ Добавить Consumer для чтения шуток из Kafka
□ Добавить Swagger/OpenAPI документацию
□ Настроить Prometheus + Grafana для метрик
□ Добавить кэширование шуток (Redis)
□ Написать интеграционные тесты с Testcontainers
□ Добавить обработку ошибок с Dead Letter Topic (DLT)


## 📝 
## Лицензия

#### Проект распространяется под лицензией MIT.


## 📞
### Контакты
* Contact: Sergey Chekryzhov
* Email; sergeyh510@gmail.com
* GitHub: sergeyh510-alt
* Project: Joke-producer
* LinkedIn: www.linkedin.com/in/sergey-chekryzhov-a38778217
* Telegram: @SergeyChekryzhov

## ⭐ Поддержка

#### Если проект оказался полезным — поставь ⭐ на GitHub!

* | Примечание: Для работы приложения требуется запущенный Kafka брокер. 
* | Используйте Docker Compose из раздела Установка и запуск.









