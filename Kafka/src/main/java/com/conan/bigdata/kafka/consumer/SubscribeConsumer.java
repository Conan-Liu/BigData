package com.conan.bigdata.kafka.consumer;

import com.conan.bigdata.kafka.util.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
        properties.put("bootstrap.servers", KafkaProperties.BROKER);
        properties.put("group.id", KafkaProperties.GROUP_ID_1);  // 订阅模式需要提供group.id
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("max.poll.records", "30000"); // 消费者一次性poll的条数,默认Integer.MAX_VALUE
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
        list.add(KafkaProperties.TOPIC);
        // 订阅的模式，是如何确定消费者个数的？
        consumer.subscribe(list);
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(100);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                System.out.println();
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
        list.add(KafkaProperties.TOPIC);
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
                    // 获取该分区已经消费的偏移量
                    committedOffset = consumer.committed(topicPartition).offset();
                    // 重置偏移量到上一次提交的位置，从下一个消息开始消费
                    consumer.seek(topicPartition, committedOffset + 1);
                }
            }
        });
    }
}
