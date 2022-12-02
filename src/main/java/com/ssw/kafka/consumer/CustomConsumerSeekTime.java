package com.ssw.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * @author ssw
 * @date 2022/12/2 9:48
 */
public class CustomConsumerSeekTime {
    public static void main(String[] args) {
        // 1.配置：创建 kafka 消费者的配置对象
        Properties properties = new Properties();
        // 1.1.连接集群：给 kafka 配置对象添加配置信息：bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu-20.04.wsl:9092,ubuntu-20.04.wsl:9093");
        // 1.2.指定key,value 反序列化 (必须)
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 1.3配置消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test9999");
        // 2.创建 kafka 消费者对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        // 3.订阅主题（可多个）
        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");
        kafkaConsumer.subscribe(topics);

        // 取分区信息
        Set<TopicPartition> assignment = kafkaConsumer.assignment();
        while (assignment.size() == 0) { //保证分区分配方案已经制定完毕，如果没有制定那么一直poll
            kafkaConsumer.poll(Duration.ofSeconds(1));
            assignment = kafkaConsumer.assignment(); //更新分区信息
        }

        // 如何指定时间消费？[时间转offset]
        HashMap<TopicPartition, Long> topicPartitionLongHashMap = new HashMap<>();

        for (TopicPartition topicPartition : assignment) {
            // 当前时刻一天前：System.currentTimeMillis() - 1 * 24 * 3600 * 1000
            topicPartitionLongHashMap.put(topicPartition, System.currentTimeMillis() - 1 * 24 * 3600 * 1000);
        }

        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = kafkaConsumer.offsetsForTimes(topicPartitionLongHashMap);

        for (TopicPartition topicPartition : assignment) {
            OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampMap.get(topicPartition);
            kafkaConsumer.seek(topicPartition, offsetAndTimestamp.offset()); //指定消费的offset
        }

        // 4.消费数据
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
        }
    }
}
