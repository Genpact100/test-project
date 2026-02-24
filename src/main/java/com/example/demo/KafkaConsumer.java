package com.example.demo;

import java.util.ArrayList;
import java.util.List;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    private final List<String> messages = new ArrayList<>();

    @KafkaListener(topics = "demo-topic", groupId = "demo-group")
    public void consume(String message) {
        messages.add(message);
        System.out.println("Received: " + message);
    }

    public List<String> getMessages() {
        return messages;
    }
}
