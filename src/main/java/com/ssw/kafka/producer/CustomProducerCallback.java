package com.ssw.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author ssw
 * @date 2022/9/8 18:07
 */
public class CustomProducerCallback {
    public static void main(String[] args) throws InterruptedException {
        // 1.配置：创建 kafka 生产者的配置对象
        Properties properties = new Properties();
        // 2.连接集群：给 kafka 配置对象添加配置信息：bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu-20.04.wsl:9092,ubuntu-20.04.wsl:9094,ubuntu-20.04.wsl:9094");
        // 3.指定key,value 序列化 (必须)：key.serializer，value.serializer
        // properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 3.创建 kafka 生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 4.调用 send 方法,发送消息
        for (int i = 0; i < 500; i++) {
            // topic: first,value: hello kafka
            kafkaProducer.send(new ProducerRecord<>("first", "hello kafka Callback" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("主题： " + metadata.topic() + " 分区： " + metadata.partition());
                    }
                }
            });
            Thread.sleep(1);
        }
        // 5.关闭资源
        kafkaProducer.close();
    }
}
