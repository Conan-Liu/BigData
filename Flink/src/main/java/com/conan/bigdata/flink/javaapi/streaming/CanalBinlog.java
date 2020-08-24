package com.conan.bigdata.flink.javaapi.streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * Canal读取Mysql Binlog，实时发送到Kafka中，Flink消费
 */
public class CanalBinlog {

    private static final String FIELD_DELIMITER = ",";

    private static class EventTimeBucketAssigner implements BucketAssigner<String, String> {
        private SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

        @Override
        public String getBucketId(String s, Context context) {
            String partitionValue;
            try {
                partitionValue = format.format(new Date(Long.parseLong(s.split(",")[1])));
            } catch (Exception e) {
                partitionValue = "00000000";
            }
            return "dt=" + partitionValue;
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // checkpoint
        // env.enableCheckpointing(1000);
        // env.setStateBackend((StateBackend) (new FsStateBackend("")));
        // CheckpointConfig config = env.getCheckpointConfig();
        // config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        // kafka source
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-test");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("mysqlbinlog", new SimpleStringSchema(), properties);
        consumer.setStartFromLatest();
        DataStreamSource<String> stream = env.addSource(consumer, "flink-kafka-consumer");

        // transform，如何转换成lamda表达式
        SingleOutputStreamOperator<String> singleStream = stream.map(x ->
                JSON.parseObject(x, Feature.OrderedField)
        ).filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject s) throws Exception {
                return "false".equals(s.getString("isDdl"));
            }
        }).map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject s) throws Exception {
                StringBuilder fields = new StringBuilder();
                // 获取最新字段值
                JSONArray data = s.getJSONArray("data");
                JSONObject json;
                for (int i = 0; i < data.size(); i++) {
                    json = data.getJSONObject(i);
                    if (json != null) {
                        fields.append(s.getLong("id"));
                        fields.append(FIELD_DELIMITER);
                        fields.append(s.getLong("es"));
                        fields.append(FIELD_DELIMITER);
                        fields.append(s.getLong("ts"));
                        fields.append(FIELD_DELIMITER);
                        fields.append(s.getString("type"));
                        for (Map.Entry<String, Object> entry : json.entrySet()) {
                            fields.append(FIELD_DELIMITER);
                            fields.append(entry.getValue());
                        }
                    }
                }
                return fields.toString();
            }
        });

        //stream.print();
        singleStream.print();

        // sink，以下条件满足其中之一就会滚动生成新的文件
        RollingPolicy<String, String> rollingPolicy = DefaultRollingPolicy.create()
                .withRolloverInterval(60 * 1000L)
                .withMaxPartSize(1024 * 1024 * 256L)
                .withInactivityInterval(60 * 1000L).build();
/*
        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(new Path(""), new SimpleStringEncoder<String>())
                .withBucketAssigner(new EventTimeBucketAssigner())
                .withRollingPolicy(rollingPolicy)
                .withBucketCheckInterval(1000).build();
*/
        //singleStream.addSink(sink);
        env.execute();
    }
}
