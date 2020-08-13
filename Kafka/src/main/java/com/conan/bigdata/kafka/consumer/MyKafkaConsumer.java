package com.conan.bigdata.kafka.consumer;

import com.conan.bigdata.kafka.util.Constants;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class MyKafkaConsumer {

    // Automatic Offset Committing
    public void autoOffset() {
        Properties props = new Properties();
        // max.poll.interval.ms 避免livelock
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);    // diamond 类型
        consumer.subscribe(Arrays.asList("foo", "bar"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }

    // Manual Offset Control
    public void manualOffset() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("foo", "bar"));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }

            // 消费一定数量后，提交offset
            if (buffer.size() >= minBatchSize) {
//                insertIntoDb(buffer);
                // 同步提交offset，阻塞执行
                consumer.commitSync();

                // 异步提交offset，可以提供一个回调方法，用来确认是否提交成功
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if(exception==null){
                            System.out.println("异步提交成功");
                        }else{
                            System.out.println("失败");
                        }
                    }
                });
                buffer.clear();
            }
        }
    }

    public static void main(String[] args) {

        Properties properties=SubscribeConsumer.getProperties();
        List<String> list = new ArrayList<>(3);
        list.add(Constants.TOPIC);
        KafkaConsumer<Integer, String> consumer=new KafkaConsumer<>(properties);
        consumer.subscribe(list);
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(100);
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.printf("thread [%s] => partition = %d, offset = %d, key = %s, value = %s\n", "aaa", record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

}
