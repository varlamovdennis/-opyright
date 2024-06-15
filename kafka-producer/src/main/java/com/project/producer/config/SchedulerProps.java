package com.project.producer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "scheduler")
@Getter
@Setter
public class SchedulerProps {

    private List<Scheduler> schedulers;

    @Getter
    @Setter
    public static class Scheduler {

        private String initialDelay;
        private String fixedRate;
        private String topic;
        private Integer keyMin;
        private Integer keyMax;
        private Integer valueMin;
        private Integer valueMax;
    }
}
