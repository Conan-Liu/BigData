package com.conan.bigdata.flink.javaapi.streaming;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 */
public class GroupProcessingTimeWindow {

    // 并发生产数据源， 是一系列的K-V对
    private static class DataSource extends RichParallelSourceFunction<Tuple2<String,Integer>>{

        private volatile boolean running=true;
        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            Random random=new Random(System.currentTimeMillis());
            while (running){
                Thread.sleep((getRuntimeContext().getIndexOfThisSubtask()+1)*1000);
                String key="类别"+(char)('A'+random.nextInt(3));
                int value=random.nextInt(10)+1;
                System.out.println(String.format("Emit:\t(%s, %d)",key,value));
                ctx.collect(new Tuple2<String, Integer>(key,value));
            }
        }

        @Override
        public void cancel() {
            running=false;
        }
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 得到Source 数据源
        DataStream<Tuple2<String,Integer>> ds=env.addSource(new DataSource());
        // 业务逻辑
        ds.keyBy(0).sum(1).keyBy(new KeySelector<Tuple2<String,Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return "";
            }
        }).fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> fold(HashMap<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
                accumulator.put(value.f0,value.f1);
                return accumulator;
            }
        }).addSink(null);
        // 样例 Sink 数据输出
        // Flink是DAG的处理模式，数据源和业务逻辑只是用来画DAG，然后execute时，是根据DAG来执行的
        // 也就是说这里的 ds.addSink 在DAG里面是比较靠前的，紧跟着数据源的ds，上面的addSink在DAG上是比较靠后的
        // 所以执行的时候，ds.addSink会先执行， 这和以前的代码按顺序一行一行执行不一样
        // spark也是这样
        ds.addSink(new SinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                System.out.println(String.format("Get:\t(%s, %d)",value.f0,value.f1));
            }
        });

        // 触发执行，类似于spark的action
        env.execute();
    }

}