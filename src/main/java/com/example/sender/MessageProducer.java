package com.example.sender;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class MessageProducer {

    private final Logger log = LoggerFactory.getLogger(MessageProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${myapp.kafka.topic}")
    private String topic;

    @Value("${myapp.kafka.topic3}")
    private String topic3;

    @Value("${myapp.kafka.topic4}")
    private String topic4;

    @Value("${myapp.kafka.topic6}")
    private String topic6;

    @Bean
    public NewTopic topicCreation() {
        log.info("Creating new topic.");
        return TopicBuilder.name(topic6)
                .partitions(5)
                .replicas(1)
                .build();
    }

    public void sendMessage(String message) {
        log.info("MESSAGE SENT FROM PRODUCER END -> " + message);
        kafkaTemplate.send(topic, message);
    }

    public void sendMessageToTopic3(String message) {
        log.info("Message sent from producer to topic3 -> " + message);
        kafkaTemplate.send(topic3, message);
    }

    public void sendMessageToTopic4(String message) {
        log.info("Message sent from producer to topic4 -> " + message);
        kafkaTemplate.send(topic4, message);
    }

    public void sendMessageToTestTopic(String message) {
        log.info("Message sent from producer to testTopic -> " + message);
        kafkaTemplate.send(topic6, message);
    }
}
