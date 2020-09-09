package com.conan.bigdata.flink.scalaapi.streaming

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction

/**
  * Flink通过两阶段提交协议实现Exactly Once
  * [[FlinkKafkaProducer]]已经通过继承抽象类[[TwoPhaseCommitSinkFunction]]实现两阶段提交，可以直接使用
  */
object KafkaTwoPhaseExp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置checkpoint属性
        env.setStateBackend(new FsStateBackend("file:///Users/mw/temp/flink/checkpoint/twophaseexp"))
        env.enableCheckpointing(1000)
        val ckConfig: CheckpointConfig = env.getCheckpointConfig
        ckConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        ckConfig.setMinPauseBetweenCheckpoints(500)
        ckConfig.setCheckpointTimeout(60000)
        ckConfig.setFailOnCheckpointingErrors(false)
        ckConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        ckConfig.setMaxConcurrentCheckpoints(1)

        val textStream: DataStream[String] = env.socketTextStream("localhost", 9999)
        textStream.print()

        // 输出到kafka
        val topic = "test"
        val prop = new Properties()
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        // FlinkProducer作为kafka client的事务超市时间不能大于kafka的事务超时时间
        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60000 * 15 + "")
        textStream.addSink(new FlinkKafkaProducer[String](
            topic,
            new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
            prop,
            Semantic.EXACTLY_ONCE // 设置kafka的一次性语义
        ))

        env.execute("TwoPhaseExp")
    }
}
