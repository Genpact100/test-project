package com.example.demo;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaConsumer kafkaConsumer;

    private static final String TOPIC = "demo-topic";

    @GetMapping("/publish")
    public String sendMessage(@RequestParam("message") String message) {
        kafkaTemplate.send(TOPIC, message);
        return "Message sent to Kafka: " + message;
    }

    @GetMapping("/messages")
    public ResponseEntity<List<String>> getMessages() {
        return  ResponseEntity.ok(kafkaConsumer.getMessages());
    }
    @GetMapping("/stream")
    public SseEmitter streamMessages() {
        return kafkaConsumer.addEmitter();
    }
}
