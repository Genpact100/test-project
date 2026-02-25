package com.example.demo;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Service
public class KafkaConsumer {

    private final List<String> messages = new CopyOnWriteArrayList<>();
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    @KafkaListener(topics = "demo-topic", groupId = "demo-group")
    public void consume(String message) {
        messages.add(message);
        System.out.println("Received: " + message);

        // Push to all connected clients
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(message);
            } catch (IOException e) {
                emitter.complete();
            }
        }
    }

    public List<String> getMessages() {
        return messages;
    }

    public SseEmitter addEmitter() {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        emitters.add(emitter);

        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));

        return emitter;
    }
}