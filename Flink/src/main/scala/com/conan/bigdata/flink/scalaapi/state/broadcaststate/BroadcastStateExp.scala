package com.conan.bigdata.flink.scalaapi.state.broadcaststate

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * 需求：
  * 假设一个需求，需要实时过滤出配置中的用户，并在事件流中补全这批用户的基础信息
  */
class BroadcastStateExp {

    def main(args: Array[String]): Unit = {
        val env=StreamExecutionEnvironment.getExecutionEnvironment
        // 配置消费kafka中的事件流数据source
        val prop=new Properties()
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
        val sourceDS=env.addSource(new FlinkKafkaConsumer[String]("",new SimpleStringSchema(),prop))

        // 使用ProcessFunction来处理事件流中的数据
        val tupleDS=sourceDS.process(
            new ProcessFunction[String,(String,String,String,Int)] {

                // 自定义业务处理逻辑
                override def processElement(value: String, ctx: ProcessFunction[String, (String, String, String, Int)]#Context, out: Collector[(String, String, String, Int)]):Unit = {
                    val parseObject: JSONObject = JSON.parseObject(value)
                    val userId=parseObject.getString("userID")
                    val eventTime=parseObject.getString("eventTime")
                    val eventType=parseObject.getString("eventType")
                    val productID=parseObject.getIntValue("productID")
                    out.collect((userId,eventTime,eventType,productID))
                }


            }
        )

        // 添加peizh
        val collection: DataStream[String] = env.fromCollection(Array(""))
        // 该数据源需要广播出去，作为broadcaststate来使用，需要使用一个MapState
        val descriptor: MapStateDescriptor[String, (String, Int)] = new MapStateDescriptor[String,(String,Int)]("broadcaststate",classOf[String],classOf[(String,Int)])

        val broadcast: BroadcastStream[String] = collection.broadcast(descriptor)

        val connect: BroadcastConnectedStream[(String, String, String, Int), String] = tupleDS.connect(broadcast)

        val process: Any = connect.process(new MyProcessFunction)

        collection.process(
            new BroadcastProcessFunction[] {}
        )
    }
}


class MyProcessFunction extends BroadcastProcessFunction[(String,String,String,Int),(String,(String,Int)),(String,String,String,Int,String,Int)]{

    // 定义broadcastState的描述器
    val descriptor = new MapStateDescriptor[(String,(String,Int))]("state",classOf[String],classOf[(String,Int)])

    // 处理广播流的方法
    // 事件流的数据获取广播流中的数据，需要借助于state，在该方法中把广播流数据存入到state中，在processElement方法中获取数据
    override def processBroadcastElement(value: (String, (String, Int)), ctx: BroadcastProcessFunction[(String, String, String, Int), (String, (String, Int)), (String, String, String, Int, String, Int)]#Context, out: Collector[(String, String, String, Int, String, Int)]): Unit = {

        // 根据描述器获取broadcastState数据
        val state: BroadcastState[String, (String, Int)] = ctx.getBroadcastState(descriptor)
        // 把广播流中的数据存入mapstate
        state.put(value._1,value._2)
    }

    // 处理事件流的方法
    override def processElement(value: (String, String, String, Int), ctx: BroadcastProcessFunction[(String, String, String, Int), (String, (String, Int)), (String, String, String, Int, String, Int)]#ReadOnlyContext, out: Collector[(String, String, String, Int, String, Int)]): Unit = {

        val state: ReadOnlyBroadcastState[String, (String, Int)] = ctx.getBroadcastState(descriptor)
        // 根据userId去state中取出其它数据，可能不包含
        val bool=state.contains(value._1)
        if(bool) {
            val tuple = state.get(value._1)
            out.collect((value._1,value._2,value._3,value._4,tuple._1,tuple._2))
        }else{
            // 可以丢弃，可以补null
        }
    }
}