package com.project.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableKafka
public class KafkaConfig {

    private final KafkaTopicProps topicProperties;

    @Autowired
    public KafkaConfig(KafkaTopicProps topicProperties) {
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
}
