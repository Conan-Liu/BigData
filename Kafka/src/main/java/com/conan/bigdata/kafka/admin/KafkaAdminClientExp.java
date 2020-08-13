package com.conan.bigdata.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 */
public class KafkaAdminClientExp {

    private static final String BROKER_LIST = "10.0.24.41:9092,10.0.24.42:9092,10.0.24.52:9092";
    private static final String TOPIC_NAME = "dataflow";
    private static final String TOPIC_NAME_LIST = "test_zzy,dataflow";

    public static void main(String[] args) throws Exception {

        System.out.println("AdminClient 示例...");
        // showAdminClient();

        System.out.println("手动assign打印 offset...");
        // manualPartitionOffset();

        System.out.println("subscribe自动打印 offset...");
        subscribePartitionOffset();
    }

    // 获取 AdminClient
    public static AdminClient getAdminClient() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BROKER_LIST);
        return AdminClient.create(properties);
    }

    public static void showAdminClient() throws Exception {
        AdminClient adminClient = getAdminClient();
        // 默认false, 不包含Kafka内部的Topic
        ListTopicsResult listTopicsResult = adminClient.listTopics(new ListTopicsOptions().listInternal(false));
        Map<String, TopicListing> mapKafkaFuture = listTopicsResult.namesToListings().get(10, TimeUnit.SECONDS);
        int count1 = 0;
        for (Map.Entry<String, TopicListing> map : mapKafkaFuture.entrySet()) {
            System.out.println(map.getValue());
            count1++;
        }
        System.out.println("Topic数 : " + count1);
        List<String> list = new ArrayList<>();
        list.add("test_zzy");
        list.add("dataflow");
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(list);
        Map<String, TopicDescription> all = describeTopicsResult.all().get(10, TimeUnit.SECONDS);
        int count2 = 0;
        for (Map.Entry<String, TopicDescription> map : all.entrySet()) {
            TopicDescription value = map.getValue();
            System.out.println(map.getKey() + "\t" + value.name());
            // 美化打印
            List<TopicPartitionInfo> partitions = value.partitions();
            for (TopicPartitionInfo partitionInfo : partitions) {
                System.out.println(partitionInfo);
            }
            count2++;
        }
        System.out.println("Topic数 : " + count2);
    }

    /**
     * 想要获取最新的offset，需要定义一个Consumer
     * assign()手动处理offset
     */
    public static void manualPartitionOffset() {

        Properties props = new Properties();
        // max.poll.interval.ms 避免livelock
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("group.id", "test_getlatestpartition");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 消费者组读取Topic的分区信息
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(TOPIC_NAME);
        List<TopicPartition> list = new ArrayList<>();
        for (PartitionInfo info : partitionInfos) {
            TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, info.partition());
            list.add(topicPartition);
        }
        // 消费者组根据之前读取的分区信息，安排消费者消费指定的分区，如果不安排的话，则会报错
        consumer.assign(list);
        // 每个分区上的消费者指针移动到最末尾，重置消费者偏移量
        // consumer.seekToEnd(list);
        consumer.seekToBeginning(list);
        for (TopicPartition partition : list) {
            // 返回下一次拉取的位置
            System.out.println(consumer.position(partition));
        }

        // 获取分区的最后offset，如果分区不存在则会阻塞，不会改变消费者的position，也就是不会seek操作
        // 注意这个最后offset，是最后一条信息offset + 1
        // 推荐这种方式，与命令行kafka-consumer-groups.sh的 LOG-END-OFFSET 一致
        Map<TopicPartition, Long> topicPartitionLongMap = consumer.endOffsets(list);
        for (Map.Entry<TopicPartition, Long> map : topicPartitionLongMap.entrySet()) {
            System.out.println(map.getKey() + "\t" + map.getValue());
        }
    }

    /**
     * 想要获取最新的offset，需要定义一个Consumer
     * subscribe自动处理offset
     * 注意，一定要拉取一次，kafka才能分配分区和消费者，也就是说订阅可以看成是lazy模式
     */
    public static void subscribePartitionOffset() {

        Properties props = new Properties();
        // max.poll.interval.ms 避免livelock
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("group.id", "test_getlatestpartition");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(TOPIC_NAME));
        // 一定要拉取一次才能分配
        consumer.poll(100);
        Set<TopicPartition> assignment = consumer.assignment();
        Map<TopicPartition, Long> topicPartitionLongMap = consumer.endOffsets(assignment);
        for (Map.Entry<TopicPartition, Long> map : topicPartitionLongMap.entrySet()) {
            System.out.println(map.getKey() + "\t" + map.getValue());
        }
    }

    /**
     * 消费者组信息
     * 不过现在的版本（目前最新1.1.0）并没有提供类似describeConsumerGroup和listGroupOffsets的方法实现
     * 可以直接使用 kafka-consumer-groups.sh 查看
     */
    public static void consumerGroup() {
        AdminClient adminClient = getAdminClient();
    }

    /**
     * 获取消费者组成员信息
     */
    public static void consumerMember() {
        AdminClient adminClient = getAdminClient();
    }
}
