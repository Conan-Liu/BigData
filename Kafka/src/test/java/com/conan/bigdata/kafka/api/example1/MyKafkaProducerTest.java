package com.conan.bigdata.kafka.api.example1;

import com.conan.bigdata.kafka.producer.MyKafkaProducer;
import com.conan.bigdata.kafka.util.Constants;
import org.junit.Test;

public class MyKafkaProducerTest {

    @Test
    public void test1() {
        new MyKafkaProducer(Constants.TOPIC,"1").start();
    }
}