package com.conan.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by Administrator on 2017/1/12.
 */
public class NewKafkaConsumer {
    public static void main(String[] args){
        System.out.println("begin consumer");
        connectionKafka();
        System.out.println("finish consumer");
    }

    public static void connectionKafka(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.56.101:9092");
        properties.put("group.id", "testConsumer");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("test"));
        while (true){
            ConsumerRecords<String,String> records=consumer.poll(100);
            try{
                Thread.sleep(20000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
            for(ConsumerRecord<String,String> record:records){
                System.out.printf("offset = %d, key = %s, value = %s",record.offset(),record.key(),record.value());
            }
        }
    }
}
