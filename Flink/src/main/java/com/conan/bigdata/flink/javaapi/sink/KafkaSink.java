package com.conan.bigdata.flink.javaapi.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * Flink-Kafka-Sink
 * Flink写数据到Kafka，相当于Flink的Sink，或者Kafka的Producer
 * bin/kafka-topics.sh --create --zookeeper localhost:2181/kafka --replication-factor 1 --partitions 5 --topic flink
 */
public class KafkaSink {

    private static class MyKafkaSource implements SourceFunction<String> {
        private boolean isRunning = true;

        /**
         * 启动一个source，实现while方法来循环产生数据
         */
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            Random rand = new Random();
            List<String> books = new ArrayList<>();
            books.add("Pyhton从入门到放弃");
            books.add("Java从入门到放弃");
            books.add("Php从入门到放弃");
            books.add("C++从入门到放弃");
            books.add("Scala从入门到放弃");
            while (isRunning) {
                int i = rand.nextInt(5);
                ctx.collect(books.get(i));
                // 每一秒产生一条
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.addSource(new MyKafkaSource()).setParallelism(1);
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>("flink", new SimpleStringSchema(), properties);
        streamSource.addSink(producer);
        env.execute();
    }
}
