package com.ssw.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * @author ssw
 * @date 2022/10/14 18:03
 */
public class CustomProducerTransactions {
    public static void main(String[] args) {
        // 1. 配置：创建 kafka 生产者的配置对象
        Properties properties = new Properties();
        // 2. 连接集群：给 kafka 配置对象添加配置信息：bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu-20.04.wsl:9092,ubuntu-20.04.wsl:9094,ubuntu-20.04.wsl:9094");
        // 3.指定key,value 序列化 (必须)：key.serializer，value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 0位指定事物id将报错：IllegalStateException: Cannot use transactional methods without enabling transactions by setting the transactional.id configuration property
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional_id_01");

        // 3. 创建 kafka 生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // ①初始化事务
        kafkaProducer.initTransactions();
        // ②开启事务
        kafkaProducer.beginTransaction();

        try {
            // 4. 调用 send 方法,发送消息
            for (int i = 0; i < 5; i++) {
                // topic: first,value: hello kafka
                kafkaProducer.send(new ProducerRecord<>("first", "hello kafka transactions " + UUID.randomUUID())).get();
            }
            int i = 1 / 0;
            // ③提交事物
            kafkaProducer.commitTransaction();
        } catch (Exception e) {
            // 抛弃事物，回滚
            kafkaProducer.abortTransaction();
            System.out.println("出现异常，提交事物");
        }finally {
            // 5. 关闭资源
            kafkaProducer.close();
        }
    }
}
