package com.example.controller;

import com.example.repository.Message;
import com.example.sender.MessageProducer;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaRestController {

    @Autowired
    private MessageProducer producer;

    @Autowired
    private Message messageRepo;

    /**
     * Send same message to one topic.
     * Consumers consuming message: CONSUMER number 1, CONSUMER number 2
     * @param message
     * @return
     */
    @GetMapping("/send")
    public String sendMessage(@RequestParam("msg") String message) {
        producer.sendMessage(message);
        return message + " sent successfully to topic!";
    }

    /**
     * Sending message to Topic: myKafkaTest3
     * @param message
     * @return
     */
    @GetMapping("/send/topic3")
    public String sendMessageToTopic3(@RequestParam("msg") String message) {
        producer.sendMessageToTopic3(message);
        return message + " sent successfully to topic number 3!";
    }

    /**
     * Sending message to Topic: myKafkaTest4
     * @param message
     * @return
     */

    @GetMapping("/send/topic4")
    public String sendMessageToTopic4(@RequestParam("msg") String message) {
        producer.sendMessageToTopic4(message);
        return message + " sent successfully to topic number 4!";
    }

    //Read all messages
    @GetMapping("/getAll")
    public String getAllMessages() {
        return messageRepo.getAllMessages();
    }

    @GetMapping("/createTopic")
    public String createTopic() {
        NewTopic newTopic = producer.topicCreation();
        return newTopic.name() + " topic created!";
    }

    @GetMapping("/send/testTopic")
    public String sendMessageToTestTopic(@RequestParam("message") String message) {
        producer.sendMessageToTestTopic(message);
        return message + " sent to testTopic.";
    }
}
