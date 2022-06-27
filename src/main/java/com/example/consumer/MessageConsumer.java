package com.example.consumer;

import com.example.repository.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

@Component
public class MessageConsumer {

    private final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    @Autowired
    private Message messageRepo;

    /**
     * Multiple consumer groups reading from the same topic.
     * Here different groupId means different consumer group.
     *
     * @param message
     */
    @KafkaListener(topics = "${myapp.kafka.topic}", groupId = "myGroup")
    public void consume(String message) {
        log.info("MESSAGE RECEIVED AT CONSUMER number 1 -> " + message);
        messageRepo.sendMessage(message);
    }

    @KafkaListener(topics = "${myapp.kafka.topic}", groupId = "anotherGroup")
    public void consumeNew(String message) {
        log.info("Message received at consumer number 2 -> " + message);
        messageRepo.sendMessage(message);
    }

    /**
     * Consumers reading from the different topics
     */
    @KafkaListener(topics = "${myapp.kafka.topic3}", groupId = "myGroupOne")
    public void consumeMessage(String message) {
        log.info("Message received at consumer number 3 -> " + message);
        messageRepo.sendMessage(message);
    }

    @KafkaListener(topics = "${myapp.kafka.topic4}", groupId = "anotherGroupTwo")
    public void consumeMessage2(String message) {
        log.info("Message received at consumer number 4 -> " + message);
        messageRepo.sendMessage(message);
    }

    /**
     * Consumer reading from same topic testTopic having 5 partitions
     *
     * @param message
     */
    @KafkaListener(topicPartitions = @TopicPartition(topic = "${myapp.kafka.topic6}", partitions = {"0"}), groupId = "testgroupOne")
    public void consumeMessageFromTestTopicP0(String message) {
        log.info("Message received from testTopic P0 -> " + message);
        messageRepo.sendMessage(message);
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "${myapp.kafka.topic6}", partitions = {"1"}), groupId = "testgroupOne")
    public void consumeMessageFromTestTopicP1(String message) {
        log.info("Message received from testTopic P1 -> " + message);
        messageRepo.sendMessage(message);
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "${myapp.kafka.topic6}", partitions = {"2"}), groupId = "testgroupOne")
    public void consumeMessageFromTestTopicP2(String message) {
        log.info("Message received from testTopic P2 -> " + message);
        messageRepo.sendMessage(message);
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "${myapp.kafka.topic6}", partitions = {"3"}), groupId = "testgroupOne")
    public void consumeMessageFromTestTopicP3(String message) {
        log.info("Message received from testTopic P3 -> " + message);
        messageRepo.sendMessage(message);
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "${myapp.kafka.topic6}", partitions = {"4"}), groupId = "testgroupOne")
    public void consumeMessageFromTestTopicP4(String message) {
        log.info("Message received from testTopic P4 -> " + message);
        messageRepo.sendMessage(message);
    }
}
