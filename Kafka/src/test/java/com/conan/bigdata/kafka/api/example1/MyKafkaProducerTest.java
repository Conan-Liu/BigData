package com.conan.bigdata.kafka.api.example1;

import com.conan.bigdata.kafka.util.KafkaProperties;
import org.junit.Test;

/**
 * Created by Conan on 2019/4/21.
 */
public class MyKafkaProducerTest {

    @Test
    public void test1() {
        new MyKafkaProducer(KafkaProperties.TOPIC,"1").start();
    }
}