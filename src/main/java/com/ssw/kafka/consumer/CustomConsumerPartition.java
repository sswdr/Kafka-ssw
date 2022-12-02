package com.ssw.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author ssw
 * @date 2022/12/1 15:15
 */
public class CustomConsumerPartition {
    public static void main(String[] args) {
        // 1.配置：创建 kafka 消费者的配置对象
        Properties properties = new Properties();
        // 1.1.连接集群：给 kafka 配置对象添加配置信息：bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu-20.04.wsl:9092,ubuntu-20.04.wsl:9093");
        // 1.2.指定key,value 反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 1.3配置消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 2.创建 kafka 消费者对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        // 3.订阅主题对应分区
        ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(new TopicPartition("first", 0));
        kafkaConsumer.assign(topicPartitions);
        // 4.消费数据
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
        }
    }
}
