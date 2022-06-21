package com.example.repository;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class Message {

    private final List<String> list = new ArrayList<>();

    public void sendMessage(String message) {
        list.add(message);
    }

    public String getAllMessages() {
        return list.toString();
    }
}
