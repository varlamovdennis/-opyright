package com.project.consumer.config;

import com.project.consumer.listener.EventKafkaListener;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.nonNull;

@Configuration
@EnableKafka
public class KafkaConfig {

    private final static Logger log = LoggerFactory.getLogger(EventKafkaListener.class);

    private final KafkaProps kafkaProps;
    private final KafkaTopicProps topicProperties;

    @Autowired
    public KafkaConfig(KafkaProps kafkaProps, KafkaTopicProps topicProperties) {
        this.kafkaProps = kafkaProps;
        this.topicProperties = topicProperties;
    }

    @Bean
    public KafkaAdmin.NewTopics topics() {
        List<NewTopic> topics = new ArrayList<>();
        topicProperties.getTopics()
                .forEach(topic ->
                        topics.add(TopicBuilder
                                .name(topic.getName())
                                .partitions(topic.getPartitions())
                                .replicas(topic.getReplicas())
                                .build()));
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }

    @Bean
    @Qualifier("consumer-1")
    public ConcurrentKafkaListenerContainerFactory<String, String> consumer_1_ContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("consumer-1", factory));
        return factory;
    }

    @Bean
    @Qualifier("consumer-2")
    public ConcurrentKafkaListenerContainerFactory<String, String> consumer_2_ContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("consumer-2", factory));
        return factory;
    }

    @Bean
    @Qualifier("consumer-3")
    public ConcurrentKafkaListenerContainerFactory<String, String> consumer_3_ContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("consumer-3", factory));
        return factory;
    }
    @Bean
    @Qualifier("consumer-4")
    public ConcurrentKafkaListenerContainerFactory<String, String> consumer_4_ContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("consumer-4", factory));
        return factory;
    }

    private ConsumerFactory<String, Object> consumerFactory(String consumerName, ConcurrentKafkaListenerContainerFactory<String, String> factory) {
        Map<String, Object> properties = new HashMap<>(kafkaProps.buildCommonProperties());
        if (nonNull(kafkaProps.getConsumer())) {
            KafkaProperties.Consumer consumerProps = kafkaProps.getConsumer().get(consumerName);
            if (nonNull(consumerProps)) {
                properties.putAll(consumerProps.buildProperties(null));
                String concurrency = consumerProps.getProperties().get("concurrency");
                if (nonNull(concurrency)) {
                    factory.setConcurrency(Integer.parseInt(concurrency));
                }
            }
        }
        log.debug("consumer=[{}], properties=[{}]", consumerName, properties);
        return new DefaultKafkaConsumerFactory<>(properties);
    }
}
