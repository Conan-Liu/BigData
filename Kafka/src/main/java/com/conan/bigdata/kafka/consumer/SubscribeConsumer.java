package com.conan.bigdata.kafka.consumer;

import com.conan.bigdata.kafka.util.Constants;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Subscribe 订阅模式消费kafka，基于新版的Consumer {@link KafkaConsumer}
 */
public class SubscribeConsumer {
    public static void main(String[] args) {
        System.out.println("begin consumer");
        connectionKafka1();
        System.out.println("finish consumer");
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER);
        properties.put("group.id", Constants.GROUP_ID_1);  // Subscribe模式必须提供group.id
        properties.put("enable.auto.commit", "true");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "client_id"); // 这个是配置client_id，对应着kafka manager上的Consumer Instance Owner前缀
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("max.poll.records", "30000"); // 消费者最大一次性poll的条数,默认Integer.MAX_VALUE
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }

    /**
     * 订阅模式，不使用ConsumerRebalanceListener来监听rebalance
     */
    private static void connectionKafka1() {
        Properties properties = getProperties();
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(properties);

        List<String> list = new ArrayList<>(5);
        list.add(Constants.TOPIC);
        // 订阅的模式，是如何确定消费者个数的？
        consumer.subscribe(list);
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(100);
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s\n", record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    /**
     * 订阅模式，使用ConsumerRebalanceListener监听rebalance，用于当消费者发生rebalance时回调处理
     */
    private static void connectionKafka2() {
        Properties properties = getProperties();
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(properties);

        List<String> list = new ArrayList<>(5);
        list.add(Constants.TOPIC);
        // 订阅的模式，是如何确定消费者个数的？
        consumer.subscribe(list, new ConsumerRebalanceListener() {
            // 消费者rebalance开始之前、消费者停止拉取消息之后被调用(可以提交偏移量以避免数据重复消费)
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync();
            }

            // 消费者rebalance之后、消费者开始拉取消息之前被调用(可以在该方法中保证各消费者回滚到正确的偏移量，重置各消费者偏移量)
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                long committedOffset = -1;
                for (TopicPartition topicPartition : partitions) {
                    /**
                     * Kafka提供两种查询偏移量的方法，下面两种只可使用一种，同时用可以但没必要
                     */
                    // 获取该分区已经消费的偏移量
                    committedOffset = consumer.committed(topicPartition).offset();
                    // 重置偏移量到上一次提交的位置，从下一个消息开始消费
                    consumer.seek(topicPartition, committedOffset + 1);

                    // 返回下一次拉取的位置，与上面一个参数的区别就在于要不要多偏移一位 +1
                    long position = consumer.position(topicPartition);
                    consumer.seek(topicPartition, position);
                }
            }
        });
    }
}
