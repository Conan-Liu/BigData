package com.conan.bigdata.flink.javaapi.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Flink-Kafka-Source
 * Flink从Kafka读取数据，相当于Flink的Source，或者Kafka的Consumer
 */
public class KafkaSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-test");
        // 如果使用了两阶段提交，引入了事务，那么需要设置消费者的隔离级别，默认读取未提交数据
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("flink", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        DataStream<String> streamSource = env.addSource(consumer);

        streamSource.print();
        env.execute();
    }
}
