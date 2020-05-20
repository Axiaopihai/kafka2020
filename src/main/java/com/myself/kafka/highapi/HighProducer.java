package com.myself.kafka.highapi;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author zxq
 * 2020/5/19
 * 高阶生产者api
 */
public class HighProducer {

    public static void main(String[] args) {

        HighProducer myProducer = new HighProducer();
        //获取配置信息
        Properties properties = myProducer.getProperties();
        //获取kafka producer客户端发送数据
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            //封装数据
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first","kafka"+i);
            myProducer.doSend(producer,record);
        }
        //关闭资源
        producer.close();


    }

    private Properties getProperties(){
        Properties properties = new Properties();
        //必须配置
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop128:9092,hadoop129:9092,hadoop130:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //可选配置,可应用默认值
        properties.put(ProducerConfig.ACKS_CONFIG,"-1");
        properties.put(ProducerConfig.RETRIES_CONFIG,1);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1000);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,"com.myself.kafka.interceptor.CustomProducerInterceptor");
        return properties;
    }

    private void doSend(KafkaProducer<String, String> producer,ProducerRecord<String, String> record){
        //不带回调函数
        producer.send(record);
        //带回调函数
//        producer.send(record, new Callback() {
//            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                //根据发送情况指定特定操作
//                if (e==null){
//                    long offset = recordMetadata.offset();
//                    //其他操作
//                    System.out.println(offset);
//                }
//            }
//        });
        //kafka默认为异步发送数据，由于 send 方法返回的是一个 Future 对象，
        //根据 Futrue 对象的特点，我们也可以实现同步发送的效果，只需在调用 Future 对象的 get 方发即可
//        try {
//            RecordMetadata recordMetadata = producer.send(record).get();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }
    }

}
