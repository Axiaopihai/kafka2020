package com.myself.kafka.highapi;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * @author zxq
 * 2020/5/19
 * 人为手动提交offset
 */
public class DoCommitOffsetConsumer {
    public static void main(String[] args) {
        DoCommitOffsetConsumer doCommitOffsetConsumer = new DoCommitOffsetConsumer();
        Properties properties = doCommitOffsetConsumer.getProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("first"));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                System.out.println(value);
            }
            //同步提交偏移量
            consumer.commitSync();
            //异步提交偏移量
//            consumer.commitAsync();
//            consumer.commitAsync(new OffsetCommitCallback() {
//                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
//                    if (e==null){
//                        Set<TopicPartition> topicPartitions = map.keySet();
//                        for (TopicPartition topicPartition : topicPartitions) {
//                            System.out.println(map.get(topicPartition));
//                        }
//                    }
//                }
//            });
        }
    }

    private Properties getProperties(){
        Properties properties = new Properties();
        //必须配置
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop128:9092,hadoop129:9092,hadoop130:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"myself");
        //可选配置，可应用默认值
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        return properties;
    }




}
