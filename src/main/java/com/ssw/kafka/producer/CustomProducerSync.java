package com.ssw.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author ssw
 * @date 2022/9/8 18:11
 */
public class CustomProducerSync {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        // 1.配置：创建 kafka 生产者的配置对象
        Properties properties = new Properties();
        // 2.连接集群：给 kafka 配置对象添加配置信息：bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu-20.04.wsl:9092,ubuntu-20.04.wsl:9094,ubuntu-20.04.wsl:9094");
        // 3.指定key,value 序列化 (必须)：key.serializer，value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 设置 acks
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 重试次数 retries，默认是 int 最大值，2147483647
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 3.创建 kafka 生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 4.调用 send 方法,发送消息
        for (int i = 0; i < 5; i++) {
            // topic: first,value: hello kafka
            kafkaProducer.send(new ProducerRecord<>("first", "hello kafka" + i)).get();
        }
        // 5.关闭资源
        kafkaProducer.close();
    }
}
