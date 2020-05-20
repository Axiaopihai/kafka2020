package com.myself.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author zxq
 * 2020/5/20
 * 自定义生产端拦截器
 */
public class CustomProducerInterceptor implements ProducerInterceptor<String,String> {

    @Override
    public void configure(Map<String, ?> map) {
        //获取配置，初始化调用

    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        //主要的业务逻辑
        String value = producerRecord.value();
        String result = "produce+"+value;
        return new ProducerRecord<>(producerRecord.topic(),result);
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        //发送数据之后回调调用，在produce或consumer中回调函数之前调用

    }

    @Override
    public void close() {
        //关闭资源
    }


}
