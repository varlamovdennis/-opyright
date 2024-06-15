package com.project.consumer.listener;

import com.project.consumer.service.CounterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

@Service
public class EventKafkaListener {

    private final static Logger log = LoggerFactory.getLogger(EventKafkaListener.class);

    private final Environment env;
    private final CounterService counterService;
    @Qualifier("counterTaskExecutor")
    private final ThreadPoolTaskExecutor taskExecutor;

    public EventKafkaListener(Environment env, CounterService counterService, ThreadPoolTaskExecutor taskExecutor) {
        this.env = env;
        this.counterService = counterService;
        this.taskExecutor = taskExecutor;
    }

    @KafkaListener(topics = "#{'${spring.kafka.consumer.consumer-1.properties.topics}'.split(',')}",
            groupId = "${spring.kafka.consumer.consumer-1.group-id}",
            containerFactory = "consumer_1_ContainerFactory")
    public void listener_1(@Payload String message,
                           @Header(KafkaHeaders.RECEIVED_KEY) String key,
                           @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                           @Header(KafkaHeaders.RECEIVED_PARTITION) String partition,
                           @Header(KafkaHeaders.OFFSET) String offset) {
        taskExecutor.execute(() -> processMessage("consumer-1", message, key, topic, partition, offset));
    }

    @KafkaListener(topics = "#{'${spring.kafka.consumer.consumer-2.properties.topics}'.split(',')}",
            groupId = "${spring.kafka.consumer.consumer-2.group-id}",
            containerFactory = "consumer_2_ContainerFactory")
    public void listener_2(@Payload String message,
                           @Header(KafkaHeaders.RECEIVED_KEY) String key,
                           @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                           @Header(KafkaHeaders.RECEIVED_PARTITION) String partition,
                           @Header(KafkaHeaders.OFFSET) String offset) {
        taskExecutor.execute(() -> processMessage("consumer-2", message, key, topic, partition, offset));
    }

    @KafkaListener(topics = "#{'${spring.kafka.consumer.consumer-3.properties.topics}'.split(',')}",
            groupId = "${spring.kafka.consumer.consumer-3.group-id}",
            containerFactory = "consumer_3_ContainerFactory")
    public void listener_3(@Payload String message,
                           @Header(KafkaHeaders.RECEIVED_KEY) String key,
                           @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                           @Header(KafkaHeaders.RECEIVED_PARTITION) String partition,
                           @Header(KafkaHeaders.OFFSET) String offset) {
        taskExecutor.execute(() -> processMessage("consumer-3", message, key, topic, partition, offset));
    }

    @KafkaListener(topics = "#{'${spring.kafka.consumer.consumer-4.properties.topics}'.split(',')}",
            groupId = "${spring.kafka.consumer.consumer-4.group-id}",
            containerFactory = "consumer_4_ContainerFactory")
    public void listener_4(@Payload String message,
                           @Header(KafkaHeaders.RECEIVED_KEY) String key,
                           @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                           @Header(KafkaHeaders.RECEIVED_PARTITION) String partition,
                           @Header(KafkaHeaders.OFFSET) String offset) {
        taskExecutor.execute(() -> processMessage("consumer-4", message, key, topic, partition, offset));
    }

    private void processMessage(String consumerName, String message, String key, String topic, String partition, String offset) {
        log.debug("=> kafka message: consumer=[{}], message=[{}], topic=[{}], key=[{}], partition=[{}], offset=[{}], thread=[{}]",
                getConsumerGroupId(consumerName), message, topic, key, partition, offset, Thread.currentThread().getName());
        counterService.count(topic, key);
    }

    private String getConsumerGroupId(String name) {
        return env.getProperty("spring.kafka.consumer." + name + ".group-id");
    }

}
