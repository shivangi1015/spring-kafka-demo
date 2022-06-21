package com.example.controller;

import com.example.repository.Message;
import com.example.sender.MessageProducer;
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

    @GetMapping("/send")
    public String sendMessage(@RequestParam("msg") String message) {
        producer.sendMessage(message);
        return message + " sent successfully to topic!";
    }

    //Read all messages
    @GetMapping("/getAll")
    public String getAllMessages() {
        return messageRepo.getAllMessages() ;
    }
}
