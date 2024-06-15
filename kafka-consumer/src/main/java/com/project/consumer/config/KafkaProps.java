package com.project.consumer.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "spring.kafka")
@Getter
@Setter
public class KafkaProps {

    private List<String> bootstrapServers = new ArrayList<>();
    private Map<String, String> properties = new HashMap<>();
    private Map<String, KafkaProperties.Consumer> consumer;
    private KafkaProperties.Security security = new KafkaProperties.Security();

    public Map<String, Object> buildCommonProperties() {
        Map<String, Object> properties = new HashMap<>();
        if (!CollectionUtils.isEmpty(this.bootstrapServers)) {
            properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        }
        properties.putAll(this.security.buildProperties());
        if (!CollectionUtils.isEmpty(this.properties)) {
            properties.putAll(this.properties);
        }
        return properties;
    }
}
