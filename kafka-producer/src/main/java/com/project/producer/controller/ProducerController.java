package com.project.producer.controller;

import com.project.producer.service.CounterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/producer")
public class ProducerController {

    private final CounterService counterService;

    @Autowired
    public ProducerController(CounterService counterService) {
        this.counterService = counterService;
    }

    @GetMapping()
    public ResponseEntity<?> getCount() {
        return new ResponseEntity<>(counterService.getCounter(), HttpStatus.OK);
    }

    @GetMapping("/topics")
    public ResponseEntity<?> getTopics() {
        return new ResponseEntity<>(counterService.getInitialCounter(), HttpStatus.OK);
    }
}
