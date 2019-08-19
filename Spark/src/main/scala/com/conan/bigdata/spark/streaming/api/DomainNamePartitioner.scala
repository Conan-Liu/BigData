package com.conan.bigdata.spark.streaming.api

import java.net.URL

import org.apache.spark.Partitioner

/**
  * Spark 提供的 HashPartitioner 和 RgePartitioner， 也可以自定义 Partitioner
  * 比如说
  * 如果， 计算PageRank ， K 一般是 URL， 如果是一个网站内的网址， 域名一般是相同的， 而且关系很可能比较紧密
  * 但是如果使用普通的按 HashPartitioner 来分区， 那么一个网站内的网址可能被分区到不同的分区中，
  * 这个可以自定义分区， 实现同一个网站内的网址， 分到同一个分区内
  * 说白了就是同一个域名的在一个分区内
  */
class DomainNamePartitioner(numPart: Int) extends Partitioner {
    // 构造方法，确定想创建的分区数
    override def numPartitions: Int = numPart

    override def getPartition(key: Any): Int = {
        val url = new URL(key.toString).getHost
        val code = url.hashCode % numPartitions
        // String 的hashcode很可能是负数， partition id 一定要是非负数才行， 需要转一下
        if (code < 0) {
            code + numPartitions
        } else {
            code
        }
    }

    // 判断两个分区器是否相同，
    override def equals(obj: Any): Boolean = obj match {
        // 模式匹配， 类型自动转换
        case obj: DomainNamePartitioner => this.numPartitions == obj.numPartitions
        case _ => false
    }
}