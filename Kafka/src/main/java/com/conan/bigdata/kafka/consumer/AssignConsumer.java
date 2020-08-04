package com.conan.bigdata.kafka.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Assign 手动分配的模式来消费kafka，基于新版的Consumer {@link KafkaConsumer}
 */
public class AssignConsumer {

    public static void main(String[] args) {
        Properties p=new Properties();
        KafkaConsumer<String,String> consumer=new KafkaConsumer<>(p);
        List<TopicPartition> list=new ArrayList<>();
        // 手动分配消费者，如何确定消费者的数量呢 streaming如何使消费者和kafka分区数一致的？
        consumer.assign(list);

        // 手动定位offset
        consumer.seek(list.get(0),0);
    }
}
