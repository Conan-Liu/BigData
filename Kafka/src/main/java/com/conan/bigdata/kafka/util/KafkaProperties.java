package com.conan.bigdata.kafka.util;

/**
 * Created by Administrator on 2017/1/12.
 */
public interface KafkaProperties {
    String zkConnect = "master:2181";
    String groupID = "group1";
    String topic = "topic1";
    String kafkaServerURLPort = "master:9092";
    String kafkaServerURL = "master";
    int kafkaServerPort = 9092;
    int kafkaProducerBufferSize = 64 * 1024;
    int connectionTimeOut = 20000;
    int reconnectInterval = 10000;
    String topic2 = "topic2";
    String topic3 = "topic3";
    String clientID = "SimpleConsumerDemoClient";
}
