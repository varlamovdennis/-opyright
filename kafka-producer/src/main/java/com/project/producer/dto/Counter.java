package com.project.producer.dto;

import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.Map;

@Getter
@Setter
public class Counter {

    private Long total;
    private Map<String, Map<String, Long>> counter;
    private Long updatedAt;

    public Counter(Long total, Map<String, Map<String, Long>> counter) {
        this.total = total;
        this.counter = counter;
        this.updatedAt = Instant.now().getEpochSecond();
    }
}
