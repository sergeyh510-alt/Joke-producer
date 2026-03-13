package com.example.jokekafkaproducer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer; // Импортируем для конфигурации
import org.springframework.kafka.support.serializer.JacksonJsonSerializer; // Новый сериализатор

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic jokesTopic() {
        return TopicBuilder.name("jokes-topic")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // ИСПОЛЬЗУЕМ НОВЫЙ JacksonJsonSerializer
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);

        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Конфигурация для JSON
        config.put(JacksonJsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        config.put(JacksonJsonSerializer.TYPE_MAPPINGS,
                "joke:com.example.jokekafkaproducer.model.Joke");

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}

//package com.example.jokekafkaproducer.config;

//import org.apache.kafka.clients.admin.NewTopic;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.kafka.config.TopicBuilder;

//@Configuration
//public class KafkaConfig {
//    @Bean
//    public NewTopic jokesTopic() {
//        return TopicBuilder.name("jokes-topic")
//                .partitions(1)
//                .replicas(1)
//                .build();
//    }
//}