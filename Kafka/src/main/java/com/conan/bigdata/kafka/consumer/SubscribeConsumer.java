package com.conan.bigdata.kafka.consumer;

import com.conan.bigdata.kafka.util.Constants;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Subscribe 订阅模式消费kafka，基于新版的Consumer {@link KafkaConsumer}
 * <p>
 * new一个KafkaConsumer，这是消费者客户端，服务端{@link kafka.coordinator.group.GroupCoordinator}会把该消费者线程添加到消费者组Group中，并拥有一个MemberId
 * 再new一个KafkaConsumer，等于又创建了一个客户端，服务端分配一个新的MemberId，并把该消费者添加到组Group中，，这样该消费者组中有两个不同MemberId的消费者线程了
 * 由此可见{@link KafkaConsumer}是单线程消费的模式，new KafkaConsumer只能得到一个消费者线程，只拥有一个MemberId，
 * 如果想多个消费者线程同时消费Kafka，可以通过多线程模式（多进程也行），每个线程new一个KafkaConsumer，实现多MemberId消费，提高吞吐量
 * Kafka的KafkaConsumer必须保证只能被一个线程操作
 * Kafka服务端通过MemberId来标识同一个消费者组里面的消费者线程
 */
public class SubscribeConsumer {
    public static void main(String[] args) {
        System.out.println("begin consumer");
//        connectionKafka1();
//        mulitConsumer();
        // multiThreadWithSameConsumer();
        System.out.println("finish consumer");
    }

    public static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_LIST);
        properties.put("group.id", Constants.GROUP_ID_1);  // Subscribe模式必须提供group.id
        properties.put("enable.auto.commit", "true");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka_id"); // 这个是配置client_id，对应着kafka manager上的Consumer Instance Owner前缀
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
        // 订阅的模式，是如何确定消费者个数的？
        consumer.subscribe(Constants.TOPIC_LIST);
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

        // 订阅的模式，是如何确定消费者个数的？
        consumer.subscribe(Constants.TOPIC_LIST, new ConsumerRebalanceListener() {
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

    /**
     * 多线程实现多个消费者线程来消费Kafka数据，对应不同的MemberId
     */
    private static class KafkaConsumerRunnable implements Runnable {

        private String name;
        private KafkaConsumer<Integer, String> consumer;

        private KafkaConsumerRunnable(String name, Properties properties, List<String> topics) {
            this.name = name;
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(topics);
        }

        @Override
        public void run() {
            while (true) {
                ConsumerRecords<Integer, String> records = consumer.poll(100);
                for (ConsumerRecord<Integer, String> record : records) {
                    System.out.printf("thread [%s] => partition = %d, offset = %d, key = %s, value = %s\n", this.name, record.partition(), record.offset(), record.key(), record.value());
                }
            }
        }
    }

    private static void mulitConsumer() {
        Properties properties = getProperties();
        Thread t;
        // 消费者线程数大于Topic Partition数，并不会报错，只是多余的线程处于空闲状态
        for (int i = 0; i < 6; i++) {
            t = new Thread(new KafkaConsumerRunnable("t-" + i, properties, Constants.TOPIC_LIST));
            t.start();
        }
    }

    /**
     * {@link KafkaConsumer}不是线程安全的，所以不可以多线程来操作同一个KafkaConsumer实例，KafkaConsumer实例不支持多线程并发访问
     * 如下，演示的代码是错误的写法
     * 注意这是错误的，错误的，错误的，不可以这么写，不可以这么写，不可以这么写，重要的事情强调三遍
     * 这种写法，运行直接报错 java.util.ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded access
     * Spark使用时，可能会报这个错，可以设置参数 spark.streaming.kafka.consumer.cache.enabled=false，注意如果开启推测式执行，也可能报这个错
     */
    private static class MultiThreadWithSameConsumer implements Runnable{

        private KafkaConsumer<Integer,String> consumer;

        private MultiThreadWithSameConsumer(KafkaConsumer<Integer,String> consumer){
            this.consumer=consumer;
        }

        @Override
        public void run() {
            while (true) {
                ConsumerRecords<Integer, String> records = consumer.poll(100);
                for (ConsumerRecord<Integer, String> record : records) {
                    System.out.printf("thread [%s] => partition = %d, offset = %d, key = %s, value = %s\n", "tt", record.partition(), record.offset(), record.key(), record.value());
                }
            }
        }
    }
    private static void multiThreadWithSameConsumer() {
        Properties properties = getProperties();
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Constants.TOPIC_LIST);
        Thread t1=new Thread(new MultiThreadWithSameConsumer(consumer));
        Thread t2=new Thread(new MultiThreadWithSameConsumer(consumer));
        t1.start();
        t2.start();
    }
}
