package com.conan.bigdata.flink.scalaapi.streaming

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * Mysql的两阶段提交
  * 消费Kafka，处理数据保存到Mysql中，没有现成的两阶段Sink可以使用，需要自定义实现[[TwoPhaseCommitSinkFunction]]
  * WordCount演示
  */
object MysqlTwoPhaseExp {

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

        val topic = "test"
        val prop = new Properties()
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        prop.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

        val kafkaConsumer=new FlinkKafkaConsumer[String](
            topic,
            new SimpleStringSchema(),
            prop
        )
        // 设置kafka消费者的偏移量是基于checkpoint成功后再提交
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
        val kafkaDS = env.addSource(kafkaConsumer)

        kafkaDS.print("kafka数据>> ")

        val sum = kafkaDS.flatMap(_.split("\\s+")).map((_,1)).keyBy(0).sum(1)
        sum.addSink(new MysqlTwoPhaseCommit)
        env.execute("MysqlTwoPhaseExp")
    }
}

// 泛型是输入的数据格式，事务格式，context
class MysqlTwoPhaseCommit extends TwoPhaseCommitSinkFunction[(String,Int),MysqlTransactionState,Void]{

    // 开启事务
    override def beginTransaction(): MysqlTransactionState = {
        // 获取连接
        val connection: Connection = DriverManager.getConnection("","","")
        // 关闭事务默认提交，改为手动提交，checkpoint成功时flink自动提交
        connection.setAutoCommit(false)
        new MysqlTransactionState(connection)
    }

    // 执行动作
    override def invoke(transaction: MysqlTransactionState, value: (String, Int), context: Context[_]): Unit = {
        val conn=transaction.conn
        var sql="insert into t_wordcount(word,count) values(?,?) on duplicate key update count = ?"
        val prepareStatement = conn.prepareStatement(sql)
        prepareStatement.setString(1,value._1)
        prepareStatement.setInt(2,value._2)
        prepareStatement.setInt(3,value._2)
        prepareStatement.executeUpdate()
        prepareStatement.close()
    }

    override def preCommit(transaction: MysqlTransactionState): Unit = {
        // invoke中已完成，这里无须在操作
    }

    // 正常checkpoint后提交任务
    override def commit(transaction: MysqlTransactionState): Unit = {
        val conn=transaction.conn
        conn.commit()
        conn.close()
    }

    // 出现问题则回滚
    override def abort(transaction: MysqlTransactionState): Unit = {
        val conn=transaction.conn
        conn.rollback()
        conn.close()
    }
}

// 指定不给序列化
class MysqlTransactionState(@transient val conn:Connection)