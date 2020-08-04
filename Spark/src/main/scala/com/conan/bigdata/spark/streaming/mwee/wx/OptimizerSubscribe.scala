package com.conan.bigdata.spark.streaming.mwee.wx

import java.lang.Long
import java.util.Locale
import java.{util, lang => jl, util => ju}

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka010.ConsumerStrategy

import scala.collection.JavaConverters._

/**
  */
class OptimizerSubscribe[K, V](topics: ju.Collection[jl.String], kafkaParams: ju.Map[String, Object], offsets: ju.Map[TopicPartition, jl.Long]) extends ConsumerStrategy[K, V] with Logging {
    override def executorKafkaParams: ju.Map[String, Object] = kafkaParams

    override def onStart(currentOffsets: ju.Map[TopicPartition, Long]): Consumer[K, V] = {
        val consumer = new KafkaConsumer[K, V](kafkaParams)
        // 新版本的kafka才有，需要pom导入新的依赖，这里没有导入，因为有0.8的在用，避免冲突了，此处只作为演示

        /*
        consumer.subscribe(topics, new ConsumerRebalanceListener {
            override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
                var commitedOffset: Long = -1L
                for (tp <- partitions) {
                    commitedOffset = consumer.committed(tp).offset()
                    consumer.seek(tp, commitedOffset + 1)
                }
            }

            override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
                consumer.commitSync()
            }
        })
        val toSeek = if (currentOffsets.isEmpty) {
            offsets
        } else {
            currentOffsets
        }
        if (!toSeek.isEmpty) {
            // work around KAFKA-3370 when reset is none
            // poll will throw if no position, i.e. auto offset reset none and no explicit position
            // but cant seek to a position before poll, because poll is what gets subscription partitions
            // So, poll, suppress the first exception, then seek
            val aor = kafkaParams.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
            val shouldSuppress =
                aor != null && aor.asInstanceOf[String].toUpperCase(Locale.ROOT) == "NONE"
            try {
                consumer.poll(0)
            } catch {
                case x: NoOffsetForPartitionException if shouldSuppress =>
                    logWarning("Catching NoOffsetForPartitionException since " +
                            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + " is none.  See KAFKA-3370")
            }
            toSeek.asScala.foreach { case (topicPartition, offset) =>
                consumer.seek(topicPartition, offset)
            }
            // we've called poll, we must pause or next poll may consume messages and set position
            consumer.pause(consumer.assignment())
        }
        */

        consumer
    }
}
