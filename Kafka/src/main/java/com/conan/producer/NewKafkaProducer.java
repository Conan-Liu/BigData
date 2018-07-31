package com.conan.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by Administrator on 2017/1/12.
 */
public class NewKafkaProducer {

    public static void main(String[] args) {
        System.out.println("begin produce");
        connectionKafKa();
        System.out.println("finish produce");
    }

    public static void connectionKafKa(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.56.101:9092");
        // properties.put("zookeeper.connect", "192.168.56.101:2181"); //老参数
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apkaache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer=new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toBinaryString(i)));
        }
        producer.close();
    }
}
