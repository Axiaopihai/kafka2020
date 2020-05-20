package com.myself.kafka.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @author zxq
 * 2020/5/20
 * 自定义消费端拦截器
 */
public class CustomConsumerInterceptor implements ConsumerInterceptor<String,String> {

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> consumerRecords) {

        ArrayList<ConsumerRecord<String, String>> list = new ArrayList<>();
        HashMap<TopicPartition, List<ConsumerRecord<String,String>>> map = new HashMap<>();
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            String value = consumerRecord.value();
            String result = value+"+consumer";
            ConsumerRecord<String, String> record = new ConsumerRecord<>(consumerRecord.topic(),
                    consumerRecord.partition(),consumerRecord.offset(),consumerRecord.key(),result);
            list.add(record);
            TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
            map.put(topicPartition,list);
        }
        return new ConsumerRecords<>(map);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {

    }

    @Override
    public void close() {

    }


}
