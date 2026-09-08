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

**A producer service that automatically fetches random jokes from an external API and sends them to Apache Kafka with delivery confirmation.**  
`Java` · `Spring Boot` · `Kafka` · `WebFlux` · `Reactor`

| Key Feature | Description |
|-------------|-------------|
| ⚡ Reactive | Asynchronous processing with Spring WebFlux + Reactor |
| 🎯 Atomicity | Fetch joke → Send to Kafka in a single cycle |
| 🔁 Automation | Scheduler for periodic sending (every 10 seconds) |
| 📊 Diagnostics | Full set of endpoints for monitoring |
| ✅ Acknowledgment | `acks=all` — guaranteed delivery to all replicas |

## 📋 Table of Contents

- [Description](#description)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Requirements](#requirements)
- [Installation & Running](#installation--running)
- [Configuration](#configuration)
- [API Endpoints](#api-endpoints)
- [Monitoring & Diagnostics](#monitoring--diagnostics)
- [Project Structure](#project-structure)
- [Roadmap](#roadmap)
- [License](#license)
- [Contacts](#contacts)

## 🎯 Description

### Joke Kafka Producer is a Spring Boot application that:

- Fetches random jokes from the public [official-joke-api](https://official-joke-api.appspot.com/)
- Sends them to the Kafka topic `jokes-topic` with delivery acknowledgment (`acks=all`)
- Operates in two modes:
  - **Manual** — via REST request
  - **Automatic** — via scheduler (every 10 seconds)

## 🏗 Architecture

<img width="2436" height="2879" alt="deepseek_mermaid_20260908_b86474" src="https://github.com/user-attachments/assets/0016ef1d-2408-4ed7-be3a-8e22121b8b94" />


## Sequence Diagram

<img width="5731" height="3283" alt="deepseek_mermaid_20260908_ccdb23" src="https://github.com/user-attachments/assets/b85daa83-3118-423c-9b41-b13e8ba6dafc" />


## 🛠 Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Java 19 |
| Framework | Spring Boot 4.0.3 |
| Reactive | Spring WebFlux + Project Reactor |
| Kafka | Apache Kafka (Spring Kafka) |
| Serialization | Jackson JSON |
| HTTP Client | WebClient (Reactive) |
| Build Tool | Maven |
| Utilities | Lombok |

## 📦 Requirements

- Java 19 or higher
- Apache Kafka (local or remote)
- Maven 3.8+
- Internet connection (to access the joke API)

## 🚀 Installation & Running

### 1. Clone the repository

```bash
git clone https://github.com/yourname/joke-kafka-producer.git
cd joke-kafka-producer
```

### 2. Start Kafka (locally)

#### Option A: Using Docker Compose
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
### Start:
```bach
docker-compose up -d
```
#### Option B: Using an existing Kafka installation
```bach
# Start broker (KRaft mode)
bin/kafka-server-start.sh config/kraft/server.properties
```
### 3. Build and run the application
```bach
# Build
mvn clean package

# Run
mvn spring-boot:run
```
#### Or via JAR:
```bach
java -jar target/joke-kafka-producer-0.0.1-SNAPSHOT.jar
```
## ⚙️ 
## Configuration
### application.properties
```bach
#properties
# Application
spring.application.name=joke-kafka-producer

# Kafka
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JacksonJsonSerializer

# Topic
joke.kafka.topic=jokes-topic

# Scheduler (ms)
joke.scheduler.fixed-rate=10000

# Logging
logging.level.com.example.jokekafkaproducer=DEBUG
```
### Kafka settings in code (KafkaConfig.java)
|Parameter|	Value	|Description|
|--------|----------------|------------------|
|acks|	all|	Acknowledgment from all replicas|
|retries|	3|Number of retry attempts on error|
|partitions|	1	|Number of topic partitions|
|replicas	|1|	Number of replicas|

## 🔌 
## API Endpoints
### POST /api/joke/fetch-and-send

#### Fetch a random joke from the API and send it to Kafka.

### Response:
```bach
{
  "success": true,
  "message": "Joke successfully written to Kafka (acknowledged by broker)",
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
## Diagnostic endpoints (TestController)

|Method|	URL|	Description|
|--------------|---------------|----------------------|
GET|	/api/test/simple	|Check if controller is working|
GET	|/api/test/diagnostic	|Simple diagnostic check|
GET|	/api/test/full-diagnostic|	Full diagnostic (API + Kafka)|
GET	|/api/test/check-api	|Check only API|
POST|	/api/test/check-kafka|	Check only Kafka (with test joke)|
POST|	/api/test/full-cycle|	Full cycle (API → Kafka)|
GET|	/api/test/kafka-test-simple|	Check Kafka producer creation|
GET|	/simple-ping|	Simplest ping test|

## 📊 
## Monitoring & Diagnostics
### Example requests

#### Check API:
```bach
curl -X GET http://localhost:8080/api/test/check-api
```
#### Check Kafka:
```bach
curl -X POST http://localhost:8080/api/test/check-kafka
```
#### Full diagnostic report:

```bach
curl -X GET http://localhost:8080/api/test/full-diagnostic
```
#### Fetch and send a joke:
```bach
curl -X POST http://localhost:8080/api/joke/fetch-and-send
```
## 📁
##  Project Structure

```bach
joke-kafka-producer/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/example/jokekafkaproducer/
│   │   │       ├── JokeKafkaProducerApplication.java   # Entry point
│   │   │       ├── config/
│   │   │       │   └── KafkaConfig.java                # Kafka configuration
│   │   │       ├── controller/
│   │   │       │   ├── JokeController.java             # Main REST API
│   │   │       │   ├── TestController.java             # Diagnostics
│   │   │       │   └── SimpleController.java           # Simple test
│   │   │       ├── dto/
│   │   │       │   └── KafkaSendResultDto.java         # Response DTO
│   │   │       ├── model/
│   │   │       │   └── Joke.java                       # Joke model
│   │   │       └── service/
│   │   │           ├── JokeApiService.java             # HTTP client to API
│   │   │           ├── JokeKafkaProducerService.java   # Kafka sender
│   │   │           └── ScheduledJokeService.java       # Scheduler
│   │   └── resources/
│   │       ├── application.properties                  # Configuration
│   │       ├── static/                                 # Static files
│   │       └── templates/                              # Templates
│   └── test/                                           # Tests
├── pom.xml                                             # Maven dependencies
├── .gitignore
└── README.md
```

## 🗺
## Roadmap
□ Add Consumer for reading jokes from Kafka
□ Add Swagger/OpenAPI documentation
□ Set up Prometheus + Grafana for metrics
□ Add caching for jokes (Redis)
□ Write integration tests with Testcontainers
□ Add error handling with Dead Letter Topic (DLT)


## 📝 
##  License

#### This project is distributed under the MIT License.


## 📞
### Contacts
* Contact Sergey Chekryzhov
* Email sergeyh510@gmail.com
* GitHub sergeyh510-alt
* Project Joke-producer
* LinkedIn: www.linkedin.com/in/sergey-chekryzhov-a38778217
* Telegram: @SergeyChekryzhov

## ⭐ Support

#### If you find this project useful — give it a ⭐ on GitHub!

#### Note: 
A running Kafka broker is required for the application to work. 
Use Docker Compose from the Installation & Running section.
