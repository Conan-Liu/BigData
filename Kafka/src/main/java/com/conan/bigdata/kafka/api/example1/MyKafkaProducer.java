package com.conan.bigdata.kafka.api.example1;

import com.conan.bigdata.kafka.util.KafkaProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Conan on 2019/4/21.
 * Kafka 生产者
 * 定义一个多线程， 多个生产者一起发
 */
public class MyKafkaProducer extends Thread {

    private String topic;

    private KafkaProducer<Integer, String> producer;

    public MyKafkaProducer(String topic, String threadName) {
        // 构造方法里面，调用父类构造方法，必须放第一行
        super(threadName);
        this.topic = topic;
        Map<String, Object> map = new HashMap<>();
        map.put("bootstrap.servers", KafkaProperties.BROKER);
        map.put("acks", "1");  // 1 保证有一个成功发送， 容错性和速度中等， 要求不太严格的可以考虑这个
        map.put("retries", 0);
        map.put("batch.size", 16384);
        map.put("linger.ms", 1);
        map.put("buffer.memory", 33554432);
        map.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        map.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(map);
    }

    @Override
    public void run() {
        int messageNo = 1;

        while (true) {
            String message = Thread.currentThread().getName() + "_message_" + messageNo;
            System.out.println("send: " + message);
            // 这个producerRecord可以指定分区发送
            producer.send(new ProducerRecord<>(topic, messageNo, message));
            messageNo++;
            try {
                Thread.sleep(2000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        MyKafkaProducer p1 = new MyKafkaProducer(KafkaProperties.TOPIC, "first");
        MyKafkaProducer p2 = new MyKafkaProducer(KafkaProperties.TOPIC, "second");
        p1.start();
        p2.start();
    }
}