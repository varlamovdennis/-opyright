package com.project.producer.service;

import com.project.producer.config.SchedulerProps;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

import static com.project.producer.config.SchedulerProps.Scheduler;
import static com.project.producer.util.Helper.getRandom;

@Service
public class ProducerService {

    private final static Logger log = LoggerFactory.getLogger(ProducerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final SchedulerProps schedulerProps;
    private final CounterService counterService;
    @Qualifier("counterTaskExecutor")
    private final ThreadPoolTaskExecutor taskExecutor;

    @Autowired
    public ProducerService(KafkaTemplate<String, String> kafkaTemplate, SchedulerProps schedulerProps, CounterService counterService, ThreadPoolTaskExecutor taskExecutor) {
        this.kafkaTemplate = kafkaTemplate;
        this.schedulerProps = schedulerProps;
        this.counterService = counterService;
        this.taskExecutor = taskExecutor;
    }

    @Scheduled(initialDelayString = "${scheduler.schedulers[0].initial-delay}", fixedRateString = "${scheduler.schedulers[0].fixed-rate}")
    public void send_1() {
        Scheduler scheduler = schedulerProps.getSchedulers().get(0);
        sendMessage(scheduler);
    }

    @Scheduled(initialDelayString = "${scheduler.schedulers[1].initial-delay}", fixedRateString = "${scheduler.schedulers[1].fixed-rate}")
    public void send_2() {
        Scheduler scheduler = schedulerProps.getSchedulers().get(1);
        sendMessage(scheduler);
    }

    @Scheduled(initialDelayString = "${scheduler.schedulers[2].initial-delay}", fixedRateString = "${scheduler.schedulers[2].fixed-rate}")
    public void send_3() {
        Scheduler scheduler = schedulerProps.getSchedulers().get(2);
        sendMessage(scheduler);
    }

    @Scheduled(initialDelayString = "${scheduler.schedulers[3].initial-delay}", fixedRateString = "${scheduler.schedulers[3].fixed-rate}")
    public void send_4() {
        Scheduler scheduler = schedulerProps.getSchedulers().get(3);
        sendMessage(scheduler);
    }

    @Scheduled(initialDelayString = "${scheduler.schedulers[4].initial-delay}", fixedRateString = "${scheduler.schedulers[4].fixed-rate}")
    public void send_5() {
        Scheduler scheduler = schedulerProps.getSchedulers().get(4);
        sendMessage(scheduler);
    }

    private void sendMessage(Scheduler scheduler) {
        CompletableFuture<SendResult<String, String>> future;
        try {
            future = kafkaTemplate.send(scheduler.getTopic(),
                    String.valueOf(getRandom(scheduler.getKeyMin(), scheduler.getKeyMax())),
                    String.valueOf(getRandom(scheduler.getValueMin(), scheduler.getValueMax())));
        } catch (Exception e) {
            log.error("kafka error: topic=[{}]\n{}", scheduler.getTopic(), e.getMessage());
            return;
        }
        future.whenCompleteAsync((result, ex) -> {
            ProducerRecord<String, String> record = result.getProducerRecord();
            if (ex == null) {
                taskExecutor.execute(() -> {
                    RecordMetadata metadata = result.getRecordMetadata();
                    log.debug("=> kafka message: value=[{}], topic=[{}], key=[{}], partition=[{}], offset=[{}], thread=[{}]",
                            record.value(), record.topic(), record.key(), metadata.partition(), metadata.offset(), Thread.currentThread().getName());
                    counterService.count(record.topic(), record.key());
                });
            } else {
                log.error("kafka error: topic=[{}], key=[{}], value=[{}]\n" +
                        "{}", record.topic(), record.key(), record.value(), ex.getMessage());
            }
        });
    }
}
