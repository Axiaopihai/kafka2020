package com.myself.kafka.highapi;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.sql.*;
import java.time.Duration;
import java.util.*;

/**
 * @author zxq
 * 2020/5/19
 * 自定义存储offset到mysql,注意新加入消费者后，分区分配前后的操作
 */
public class StoreOffsetConsumer {

    private static Connection connection;
    private static PreparedStatement commitStatement;
    private static PreparedStatement getStatement;

    private static HashMap<TopicPartition, Long> offsetMap = new HashMap<>();

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        //初始化mysql连接
        getConnect();
        //获取配置文件
        StoreOffsetConsumer storeOffsetConsumer = new StoreOffsetConsumer();
        Properties properties = storeOffsetConsumer.getProperties();
        //获取消费者客户端并消费数据
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("first"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                //消费者重新分区分配之前调用，提交各个分区的偏移量
                try {
                    commitOffset();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                //消费者重新分区分配之后调用
                offsetMap.clear();
                for (TopicPartition topicPartition : collection) {
                    try {
                        consumer.seek(topicPartition,getOffset(topicPartition));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                System.out.println(value);
                //缓存偏移量
                offsetMap.put(new TopicPartition(record.topic(),record.partition()),record.offset());
            }
            commitOffset();
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

    private static void commitOffset() throws  SQLException {
        //自定义存储位置到mysql

        Set<Map.Entry<TopicPartition, Long>> entries = offsetMap.entrySet();
        for (Map.Entry<TopicPartition, Long> entry : entries) {
            commitStatement.setObject(1,entry.getKey().toString());
            commitStatement.setObject(2,entry.getValue());
        }
        commitStatement.execute();
    }

    private static Long getOffset(TopicPartition topicPartition) throws SQLException {
        //从mysql获取偏移量
        getStatement.setObject(1,topicPartition);
        ResultSet resultSet = getStatement.executeQuery();
        return resultSet.getLong(1);
    }

    private static void getConnect() throws ClassNotFoundException, SQLException {
        //获取连接
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/tempdb", "root", "123456");
        commitStatement = connection.prepareStatement("insert into offsetTable values (?,?)");
        getStatement = connection.prepareStatement("select offset from offsetTable where topicPartition=?");
    }

    private static void doClose() throws SQLException {
        //关闭资源
        if (getStatement!=null){
            getStatement.close();
        }
        if (commitStatement!=null){
            commitStatement.close();
        }
        if (connection!=null){
            connection.close();
        }
    }
}
