package com.ssw.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author ssw
 * @date 2022/10/14 17:24
 */
public class CustomProducerParameters {
    public static void main(String[] args) {
        // 1.配置：创建 kafka 生产者的配置对象
        Properties properties = new Properties();
        // 2.连接集群：给 kafka 配置对象添加配置信息：bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu-20.04.wsl:9092,ubuntu-20.04.wsl:9094,ubuntu-20.04.wsl:9094");
        // 3.指定key,value 序列化 (必须)：key.serializer，value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 缓冲区大小:32m，缓冲区大小，默认 32M：buffer.memory
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        // 批次大小:16k，批次大小，默认 16K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        // linger.ms，等待时间，默认 0
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // 压缩:snappy，默认 none，可配置值 gzip、snappy、 lz4 和 zstd
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        // 3.创建 kafka 生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 4.调用 send 方法,发送消息
        for (int i = 0; i < 5; i++) {
            // topic: first,value: hello kafka
            kafkaProducer.send(new ProducerRecord<>("first", "hello kafka" + i));
        }
        // 5.关闭资源
        kafkaProducer.close();
    }
}
