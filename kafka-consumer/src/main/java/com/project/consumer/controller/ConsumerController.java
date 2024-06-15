package com.project.consumer.controller;

import com.project.consumer.service.CounterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/consumer")
public class ConsumerController {

    private final CounterService counterService;

    @Autowired
    public ConsumerController(CounterService counterService) {
        this.counterService = counterService;
    }

    @GetMapping()
    public ResponseEntity<?> getCount() {
        return new ResponseEntity<>(counterService.getCounter(), HttpStatus.OK);
    }
}
