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

        // 获取数据
        String msgValues = value.toString();

        int partition;

        if (msgValues.contains("hello")){
            partition = 0;
        }else {
            partition = 1;
        }

        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}