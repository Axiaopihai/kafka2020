package com.myself.kafka.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author zxq
 * 2020/5/20
 * 自定义分区
 */
public class CustomPartition implements Partitioner {

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        //参数：String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster
        return 0;
    }

    @Override
    public void close() {

    }


}
