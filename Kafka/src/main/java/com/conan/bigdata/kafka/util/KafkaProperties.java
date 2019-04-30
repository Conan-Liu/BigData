package com.conan.bigdata.kafka.util;

/**
 * Created by Conan on 2019/4/21.
 */
public class KafkaProperties {

    public static final String ZOOKEEPER = "CentOS:2181";

    public static final String GROUP_ID_1 = "group_id_1";

    public static final String GROUP_ID_2= "group_id_2";

    public static final String TOPIC = "hellotopic";

    public static final String BROKER = "CentOS:9092";

    public static final String BROKER_HOST = "CentOS";

    public static final int BROKER_PORT = 9092;

    public static final int kafkaProducerBufferSize = 64 * 1024;

    public static final int connectionTimeOut = 20000;

    public static final int reconnectInterval = 10000;

    public static final  String CLIENT_ID = "SimpleConsumerDemoClient";

}