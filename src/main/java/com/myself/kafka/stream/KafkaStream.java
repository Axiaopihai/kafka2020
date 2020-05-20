package com.myself.kafka.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author zxq
 * 2020/5/20
 */
public class KafkaStream {

    public static void main(String[] args) {
        //获取配置文件
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop128:9092,hadoop129:9092,hadoop130:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"wordcout");
        //编写流处理逻辑
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> stream = streamsBuilder.stream("first");
        //java样式
        /*KTable<String, Long> sum = stream.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(String s) {
                String[] split = s.split("\\+");
                return Arrays.asList(split);
            }
        }).map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
            @Override
            public KeyValue<String, String> apply(String s, String s2) {
                return new KeyValue<>(s, s2.toUpperCase());
            }
        }).groupBy(new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(String s, String s2) {
                return s2;
            }
        }).count(Materialized.as("sum"));

        sum.toStream().foreach(new ForeachAction<String, Long>() {
            @Override
            public void apply(String s, Long aLong) {
                System.out.println("word:"+s+","+"count:"+aLong);
            }
        });*/
        //lambda表达式样式
        KTable<String, Long> sum = stream.flatMapValues((ValueMapper<String, Iterable<String>>) s -> {
            String[] split = s.split("\\+");
            return Arrays.asList(split);
        }).map((KeyValueMapper<String, String, KeyValue<String, String>>) (s, s2) -> new KeyValue<>(s, s2.toUpperCase()))
                .groupBy((s, s2) -> s2).count(Materialized.as("sum"));
        sum.toStream().foreach((s, aLong) -> System.out.println("word:"+s+","+"count:"+aLong));
        //运行流处理
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
    }
}

