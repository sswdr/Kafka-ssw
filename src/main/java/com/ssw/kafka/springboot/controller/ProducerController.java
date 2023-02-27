package com.ssw.kafka.springboot.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author ssw
 * @date 2023/2/27 17:11
 */
@RestController
public class ProducerController {

    @Autowired
    KafkaTemplate<String, String> kafka;

    @GetMapping("/sendKafka")
    public String sendKafka(@RequestParam(value = "msg") String msg) {
        // Kafka 模板用来向 kafka 发送数据
        kafka.send("first", msg);
        return "ok";
    }
}
