package com.conan.bigdata.spark.utils

import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.KafkaCluster
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset

/**
  * Kafka 工具类
  */
object KafkaManager {

    // scala 默认public
    def setOrUpdateOffsets(topics: Set[String], groupId: String, kc: KafkaCluster, kafkaParams: Map[String, String]): Unit = {
        for (topic <- topics) {
            var hasConsumed = true
            val partitionsE = kc.getPartitions(Set(topic))
            if (partitionsE.isLeft)
                throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get.mkString("\n")}")
            val partitions = partitionsE.right.get
            val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
            if (consumerOffsetsE.isLeft)
                hasConsumed = false
            if (hasConsumed) {
                // 消费过
                /**
                  * 如果zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
                  * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
                  * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
                  * 这时把consumerOffsets更新为earliestLeaderOffsets
                  */
                val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
                if (earliestLeaderOffsetsE.isLeft)
                    throw new SparkException(s"get earliest offsets failed: ${earliestLeaderOffsetsE.left.get.mkString("\n")}")
                val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
                val consumerOffsets = consumerOffsetsE.right.get

                // 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
                var offsets: Map[TopicAndPartition, Long] = Map()
                for (consumerOffset <- consumerOffsets) {
                    val tp = consumerOffset._1
                    val currentOffset = consumerOffset._2
                    val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
                    if (currentOffset < earliestLeaderOffset)
                        offsets += (tp -> earliestLeaderOffset)
                }
                if (!offsets.isEmpty) {
                    kc.setConsumerOffsets(groupId, offsets)
                }
            } else {
                // 没有消费过
                val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
                var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
                if (reset == Some("smallest")) {
                    leaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get
                } else {
                    leaderOffsets = kc.getLatestLeaderOffsets(partitions).right.get
                }

                val offsets = leaderOffsets.map({
                    case (tp, leaderOffset) => (tp, leaderOffset.offset)
                })
                kc.setConsumerOffsets(groupId, offsets)
            }
        }
    }
}