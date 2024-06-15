package com.project.producer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "kafka.topics")
@Getter
@Setter
public class KafkaTopicProps {

    private List<Topic> topics = new ArrayList<>();

    @Getter
    @Setter
    public static class Topic {

        private String name;
        private Integer partitions;
        private Short replicas;
    }
}
