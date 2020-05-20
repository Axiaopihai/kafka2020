package com.myself.kafka.highapi;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author zxq
 * 2020/5/19
 * 高阶消费者api
 */
public class HighConsumer {

    public static void main(String[] args) {
        HighConsumer highConsumer = new HighConsumer();
        //获取配置文件
        Properties properties = highConsumer.getProperties();
        //获取kafka consumer消费端并拉取数据
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("first"));
//        ConsumerRecords<String, String> records = consumer.poll(1000);
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
        for (ConsumerRecord<String, String> record : records) {
            String value = record.value();
            System.out.println(value);
        }
        //关闭资源
        consumer.close();
    }

    private Properties getProperties(){
        Properties properties = new Properties();
        //必须配置
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop128:9092,hadoop129:9092,hadoop130:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"myself");
        //可选配置，可应用默认值
        //当没有offset,或者offset失效时起作用
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,"com.myself.kafka.interceptor.CustomConsumerInterceptor");
        return properties;
    }


}
