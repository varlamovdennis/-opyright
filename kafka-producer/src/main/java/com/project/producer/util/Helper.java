package com.project.producer.util;

import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Component
public class Helper {

    public static Integer getRandom(int min, int max) {
        return ThreadLocalRandom.current().nextInt(max - min + 1) + min;
    }
}
