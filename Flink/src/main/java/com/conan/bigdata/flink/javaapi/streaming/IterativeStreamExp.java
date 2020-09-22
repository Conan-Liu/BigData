package com.conan.bigdata.flink.javaapi.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@link IterativeStream} 迭代流演示
 */
public class IterativeStreamExp {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        DataStreamSource<Long> input = senv.generateSequence(1, 10);

        // 基于输入流构建IterativeStream（流迭代）
        IterativeStream<Long> iterate = input.iterate();
        // 定义迭代逻辑（map，flatMap等算子）
        DataStream<Long> map = iterate.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println(">> "+value);
                return value - 1;
            }
        });

        // 定义反馈逻辑（从迭代过的流中过滤出符合条件的元素组成部分流反馈给迭代头进行重复计算的逻辑）
        DataStream<Long> greaterThenZero = map.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 0;
            }
        });

        // 调用closeWith方法关闭一个迭代，继续下一次迭代
        iterate.closeWith(greaterThenZero);

        // 定义终止迭代逻辑（符合条件的元素被发送到下游算子，而不是继续下一次迭代）
        DataStream<Long> lessThenZero=map.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value<=0;
            }
        });

        lessThenZero.print();

        senv.execute("IterativeStreamExp");
    }
}
