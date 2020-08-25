package com.conan.bigdata.flink.javaapi.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 *
 * TODO... java和scala版本，都报这个错 The operator name Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, SumAggregator, PassThroughWindowFunction) exceeded the 80 characters length limit and was truncated
 *
 *
 * 八股文编程
 * 1. 创建一个execution environment
 * 2. 创建加载源数据Source
 * 3. 对数据进行Transformation（这一步可以没有）
 * 4. 指定计算结果的输出Sink
 * 5. 必须调用senv.execute()触发程序执行
 */
public class NetworkWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        senv.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> streamSource = senv.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapDS = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] ss = value.split("\\s+");
                for (String s : ss) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        });

        // 卧槽 lambda表达式有毒吧，这么难读，还特么难写
        streamSource.flatMap((String line, Collector<String> ctx) ->
                Arrays.stream(line.split("\\s+")).forEach(word -> ctx.collect(word))
        ).returns(Types.STRING);

        KeyedStream<Tuple2<String, Integer>, Tuple> keyByDS = flatMapDS.keyBy(0);

        // 如果不指定TimeWindow，从打印结果来看，数据是累计的，指定后，在相应时间区间内计算
        // 滚动窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowDS = keyByDS.timeWindow(Time.seconds(5));
        // 滑动窗口
        keyByDS.timeWindow(Time.seconds(10), Time.seconds(5));

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowDS.sum(1);

        sum.print();


        senv.execute("NetworkWordCount");
    }
}
