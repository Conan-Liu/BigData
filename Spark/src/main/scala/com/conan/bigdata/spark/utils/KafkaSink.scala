package com.conan.bigdata.spark.utils

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.broadcast.Broadcast

/**
  * 封装的Kafka工具， Spark调用该类作为生产者往Kafka中写入数据
  * [[org.apache.kafka.clients.producer.KafkaProducer]] 不可序列化，所以直接使用会报java.io.NotSerializableException
  * 主要是因为KafkaProducer一般会在Driver端初始化，由于不能序列化，那么Executor上就不能正常调用
  *
  * 1. 首先，我们需要将KafkaProducer利用lazy val的方式进行包装如下
  */
class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {

    lazy val producer = createProducer()

    def send(topic: String, key: K, value: V): Future[RecordMetadata] =
        producer.send(new ProducerRecord[K, V](topic, key, value))

    def send(topic: String, value: V): Future[RecordMetadata] =
        producer.send(new ProducerRecord[K, V](topic, value))
}

object KafkaSink {
    def apply[K, V](config: Properties): KafkaSink[K, V] = {
        val createProducerFunc = () => {
            val producer = new KafkaProducer[K, V](config)
            // 注册关闭的钩子来关闭KafkaProducer，关闭的钩子在executor的JVM关闭之前执行，KafkaProducer将刷新所有缓冲的消息
            sys.addShutdownHook(() => {
                producer.close()
            })
            producer
        }
        new KafkaSink[K, V](createProducerFunc)
    }


    // 下面演示如何用，在需要的地方广播这个KafkaSink变量即可
    def main(args: Array[String]): Unit = {
        // Driver端广播变量
        val kafkaProducerBC: Broadcast[KafkaSink[String, String]] = {
            val kafkaParams = new Properties()
            kafkaParams.setProperty("", "")

            // sc.broadcast(KafkaSink[String,String](kafkaParams))
            null
        }

        // Executor端获取广播变量，然后生产者发送数据
        val bc = kafkaProducerBC.value
        // bc.send()
    }
}