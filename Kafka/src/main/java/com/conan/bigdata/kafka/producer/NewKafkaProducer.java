package com.conan.bigdata.kafka.producer;

import com.conan.bigdata.kafka.util.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class NewKafkaProducer {

    public static void main(String[] args) {
        System.out.println("begin produce");
        connectionKafKa();
        System.out.println("finish produce");
    }

    public static void connectionKafKa(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", Constants.BROKER);
        // properties.put("zookeeper.connect", "192.168.56.101:2181"); //老参数
        properties.put("acks", "all");  // all 的安全系数最高，和 -1 等同，表示全部分区有反馈才行
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 生产者根据key来计算hash进行分区， 如果是null的话， 分区则不是hash， 而是一个固定时间间隔，随机选择分区
        KafkaProducer<Integer, String> producer=new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(Constants.TOPIC, i, Integer.toBinaryString(i)));
        }
        producer.close();
    }
}
