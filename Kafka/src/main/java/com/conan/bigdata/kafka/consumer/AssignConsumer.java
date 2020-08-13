package com.conan.bigdata.kafka.consumer;

import com.conan.bigdata.kafka.util.Constants;
import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

import java.util.*;

/**
 * Assign 手动分配的模式来消费kafka，基于新版的Consumer {@link KafkaConsumer}
 */
public class AssignConsumer {

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_LIST);
        properties.put("enable.auto.commit", "false");
        properties.put("group.id", Constants.GROUP_ID_2);  // Assign模式可选的group.id，方便更新offset到zookeeper
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "client_id"); // 这个是配置client_id，对应着kafka manager上的Consumer Instance Owner前缀
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("max.poll.records", "30000"); // 消费者最大一次性poll的条数,默认Integer.MAX_VALUE
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }

    public static void main(String[] args) {
        /**
         * 获取Topic的Partition对应的Offset
         * Kafka已经不在推荐老版本{@link ZkUtils}，
         * 推荐使用{@link KafkaZkClient}来替代，注意这个类的主构造是私有构造函数，只能通过其伴生对象的apply方法调用，
         * Java调用Scala方法时，Scala方法的默认值不起作用，最后两个参数是默认值，自己手动传入才行
         */
        KafkaZkClient zkClient = KafkaZkClient.apply(Constants.ZOOKEEPER, false, 1000, 1000, 10, Time.SYSTEM, "kafka.server", "SessionExpireListener");

        getAllBroker(zkClient);
        getAllTopics(zkClient);
        getTopicPartitions(zkClient, "hello1");
        getPartitionOffsets(zkClient, "hello");

        Properties p = getProperties();
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(p);

        Map<TopicPartition, Long> partitionOffsets = getPartitionOffsets(zkClient, Constants.TOPIC);
        TopicPartition tp = new TopicPartition(Constants.TOPIC, 2);
        // partitionOffsets.remove(tp);

        // 手动分配消费者，如何确定消费者的数量呢 streaming如何使消费者和kafka分区数一致的？
        consumer.assign(partitionOffsets.keySet());

        // 手动定位offset
        for (Map.Entry<TopicPartition, Long> map : partitionOffsets.entrySet()) {
            consumer.seek(map.getKey(), map.getValue());
        }

        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(100);
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s\n", record.partition(), record.offset(), record.key(), record.value());
            }

            Map<TopicPartition, Long> topicPartitionLongMap = consumer.endOffsets(partitionOffsets.keySet());
            setPartitionOffsets(zkClient, topicPartitionLongMap);

        }

    }


    /**
     * 获取集群所有Broker
     * {@link JavaConversions} 实现scala和java集合的转换
     */
    private static void getAllBroker(KafkaZkClient zkClient) {
        Collection<Broker> brokers = JavaConversions.asJavaCollection(zkClient.getAllBrokersInCluster());
        for (Broker broker : brokers) {
            System.out.println(broker);
        }
    }

    /**
     * 获取集群所有Topic
     * {@link JavaConverters} 实现scala和java集合的转换
     */
    private static void getAllTopics(KafkaZkClient zkClient) {
        List<String> topics = JavaConverters.seqAsJavaListConverter(zkClient.getAllTopicsInCluster()).asJava();
        for (String topic : topics) {
            System.out.println(topic);
        }
    }

    /**
     * 获取指定Topic的所有Partition
     */
    private static List<TopicPartition> getTopicPartitions(KafkaZkClient zkClient, String topicName) {
        boolean exists = zkClient.topicExists(topicName);
        List<TopicPartition> list = new ArrayList<>(10);
        if (exists) {
            List<String> partitions = JavaConverters.seqAsJavaListConverter(zkClient.getChildren("/brokers/topics/" + topicName + "/partitions")).asJava();
            for (String p : partitions) {
                list.add(new TopicPartition(topicName, Integer.parseInt(p)));
            }
            return list;
        } else {
            System.out.println("topic not found !!!");
        }
        return list;
    }

    /**
     * 获取指定Topic的所有Partition对应的Offset
     * 这里Java和Scala混合编程，注意这里把
     */
    private static Map<TopicPartition, Long> getPartitionOffsets(KafkaZkClient zkClient, String topicName) {
        Option<Object> topicPartitionCount = zkClient.getTopicPartitionCount(topicName);
        Map<TopicPartition, Long> map = new HashMap<>(16);
        if (topicPartitionCount.isEmpty()) {
            System.out.println("topic partition not found !!!");
        } else {
            // 从Zookeeper中获取Topic的所有Partition
            List<TopicPartition> topicPartitions = getTopicPartitions(zkClient, topicName);
            long offset;
            for (TopicPartition tp : topicPartitions) {
                Option<Object> consumerOffset = zkClient.getConsumerOffset(Constants.GROUP_ID_2, tp);
                if (consumerOffset.nonEmpty()) {
                    // 已经保存了offset
                    offset = (long) consumerOffset.get();
                } else {
                    // 初始化或者新增的分区，offset从0开始
                    offset = 0;
                }
                map.put(tp, offset);
            }
        }
        return map;
    }

    /**
     * 设置指定Topic所有Partition的Offset
     */
    private static void setPartitionOffsets(KafkaZkClient zkClient, Map<TopicPartition, Long> topicPartitionLongMap) {
        for (Map.Entry<TopicPartition, Long> map : topicPartitionLongMap.entrySet()) {
            zkClient.setOrCreateConsumerOffset(Constants.GROUP_ID_2, map.getKey(), map.getValue());
        }
    }

    /**
     * 手动分配Topic的Partition给指定的消费者线程处理
     */
    private static void assignPartitionToConsumer(){

    }
}
