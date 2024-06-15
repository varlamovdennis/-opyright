package com.project.consumer.service;

import com.project.consumer.config.SchedulerProps;
import com.project.consumer.dto.Counter;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class CounterService {

    private final static Logger log = LoggerFactory.getLogger(CounterService.class);

    private static final Map<String, List<String>> COUNTER_KEYS = new HashMap<>();

    private static final Map<String, AtomicLong> counter = new ConcurrentHashMap<>();

    @Autowired
    public CounterService(SchedulerProps schedulerProps) {
        for (SchedulerProps.Scheduler scheduler : schedulerProps.getSchedulers()) {
            String topic = scheduler.getTopic();
            List<String> keys = new ArrayList<>();
            for (int i = scheduler.getKeyMin(); i <= scheduler.getKeyMax(); i++) {
                keys.add(String.valueOf(i));
            }
            COUNTER_KEYS.put(topic, keys);
        }
    }

    public void count(String topic, String key) {
        String counterKey = getKey(topic, key);
        long count = counter.computeIfAbsent(counterKey, k -> new AtomicLong()).incrementAndGet();
        log.debug("+> count: topic=[{}], key=[{}], count=[{}], thread=[{}]",
                topic, key, count, Thread.currentThread().getName());
    }

    public Counter getCounter() {
        Map<String, Map<String, Long>> res = new HashMap<>();
        COUNTER_KEYS.forEach((topic, keys) -> {
                    Map<String, Long> count = new HashMap<>();
                    keys.forEach(key -> count.put(key, counter.getOrDefault(getKey(topic, key), new AtomicLong()).get()));
                    res.put(topic, count);
                }
        );
        Long total = res.values().stream()
                .flatMap(m -> m.values().stream())
                .mapToLong(Long::longValue)
                .sum();
        log.debug("===> TOTAL: {}, {}", total, res);
        return new Counter(total, res);
    }

    @PreDestroy
    public void destroy() {
        getCounter();
    }

    private String getKey(String... keys) {
        return String.join(":", keys);
    }
}
