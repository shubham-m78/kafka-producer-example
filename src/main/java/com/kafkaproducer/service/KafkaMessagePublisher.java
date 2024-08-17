package com.kafkaproducer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private final String topicOne = "topic-one";

    public void sendMessageToTopic(String message) {

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("topic-two", message);

        future.whenComplete((result, exception) -> {
            if (exception == null) {
                System.out.println("Sent message=[" + message + "] " +
                        "with offset=[" + result.getRecordMetadata().offset() + "] " +
                        "in Partition Number: " + result.getRecordMetadata().partition());
            } else {
                System.out.println("Unable to send message=[" + message + "] " +
                        "due to : " + exception.getMessage());
            }
        });
    }
}
