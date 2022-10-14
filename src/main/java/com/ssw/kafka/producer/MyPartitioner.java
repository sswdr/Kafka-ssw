package com.ssw.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author ssw
 * @date 2022/9/8 19:44
 */
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String msgValues = value.toString(); // 获取数据
        int partition;
        // 如果数据包含hello，发送到0分区，否则1分区
        if (msgValues.contains("hello")) {
            partition = 0;
            System.out.println("hello内容进来了，发送到0分区");
        }
        else partition = 1;
        return partition;
    }
    @Override
    public void close() {}
    @Override
    public void configure(Map<String, ?> configs) {}
}