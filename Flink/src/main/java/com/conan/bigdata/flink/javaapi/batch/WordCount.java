package com.conan.bigdata.flink.javaapi.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 八股文编程
 * 1. 创建一个execution environment
 * 2. 创建加载源数据Source
 * 3. 对数据进行Transformation（这一步可以没有）
 * 4. 指定计算结果的输出Sink
 * 5. 触发程序执行
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 1. 创建一个execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 创建加载源数据Source
        DataSource<String> stringDataSource = env.fromElements("hadoop word hadoop spark flink flink word");

        // 3. 对数据进行Transformation
        MapOperator<String, Tuple2<String, Integer>> map = stringDataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String s : value.split("\\s+"))
                    out.collect(s);
            }
        }).map(x -> new Tuple2<>(x, 1));

        AggregateOperator<Tuple2<String, Integer>> sum = map.groupBy(0).sum(1);

        // 4. 指定计算结果的输出Sink
        sum.print();

        // 5. 触发程序执行
        env.execute("WordCount");
    }
}
