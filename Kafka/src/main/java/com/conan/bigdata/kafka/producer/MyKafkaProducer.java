package com.conan.bigdata.kafka.producer;

import com.conan.bigdata.kafka.util.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

/**
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
        map.put("bootstrap.servers", Constants.BROKER);
        map.put("acks", "1");  // 1 保证有一个成功发送,容错性和速度中等,要求不太严格的可以考虑这个,all 的安全系数最高,和 -1 等同,表示全部分区有反馈才行
        map.put("retries", 0);
        map.put("batch.size", 16384); // 每个Batch要存放batch.size大小的数据后，才可以发送出去。比如说batch.size默认值是16KB，那么里面凑够16KB的数据才会发送
        map.put("linger.ms", 1); // 一个Batch被创建之后，最多过多久，不管这个Batch有没有写满，都必须发送出去了
        map.put("buffer.memory", 33554432); // 通过KafkaProducer发送出去的消息都是先进入到客户端本地的内存缓冲里，然后把很多消息收集成一个一个的Batch，再发送到Broker上去的
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
            // 这个ProducerRecord可以指定分区发送
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
        MyKafkaProducer p1 = new MyKafkaProducer(Constants.TOPIC, "first");
        MyKafkaProducer p2 = new MyKafkaProducer(Constants.TOPIC, "second");
        p1.start();
        p2.start();
    }
}