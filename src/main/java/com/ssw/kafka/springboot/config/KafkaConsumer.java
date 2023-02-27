package com.ssw.kafka.springboot.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

import javax.xml.bind.Marshaller;

/**
 * @author ssw
 * @date 2023/2/27 17:22
 */
@Configuration
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(Marshaller.Listener.class);

    // 指定要监听的 topic
    @KafkaListener(topics = "first")
    public void consumeTopic(String msg) { // 参数: 收到的 value
        log.info("收到kafka的first的topic信息: " + msg);
    }
}
