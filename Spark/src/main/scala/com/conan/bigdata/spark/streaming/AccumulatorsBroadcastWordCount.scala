package com.conan.bigdata.spark.streaming

import com.conan.bigdata.spark.rdbms.{DML, JDBCPool}
import com.conan.bigdata.spark.streaming.utils.BroadcastWrapperObject
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.util.LongAccumulator

/**
  * Created by Conan on 2019/5/9.
  */
object AccumulatorsBroadcastWordCount {

    @volatile private var broadcast: Broadcast[Seq[String]] = null
    @volatile private var longAccumulator: LongAccumulator = null

    /**
      * 广播一个可读的变量， 或者在所有机器上都保存同一份数据集， 提高计算效率
      * 当被广播后，就不要修改它的值了， 避免节点读取的数据不一致
      *
      * 广播变量允许程序员缓存一个只读的变量在每台机器上面，而不是每个任务保存一份拷贝
      * 一个机器上可以有多个任务， 如果每个任务保存一份， 显然浪费资源
      *
      * 可序列化的对象都是可以进行广播
      * 更新广播变量的基本思路：将老的广播变量删除（unpersist），然后重新广播一遍新的广播变量
      *
      * @param sc
      * @return
      */
    def getBroadcastInstance(sc: SparkContext): Broadcast[Seq[String]] = {
        if (broadcast == null) {
            synchronized {
                if (broadcast == null) {
                    val wordBlackList = Seq("a", "b", "c")
                    broadcast = sc.broadcast[Seq[String]](wordBlackList)
                    //                    broadcast = BroadcastWrapper[Seq[String]](sc, Seq("a", "b", "c"))
                }
            }
        }
        broadcast
    }

    /**
      * 集群上运行的任务， 只能通过 add 方法增加该累加器， 不能访问到 value
      * 只有Driver 端可以通过 .value 方法来获取该累加器的值
      * 使用累加器的过程中只能使用一次action的操作才能保证结果的准确性
      *
      * 可以通过继承 [[org.apache.spark.util.AccumulatorV2]] 这个抽象类来实现自定义累加器
      *
      * @param sc
      * @return
      */
    def getLongAccumulatorInstance(sc: SparkContext): LongAccumulator = {
        if (longAccumulator == null) {
            synchronized {
                if (longAccumulator == null) {
                    longAccumulator = sc.longAccumulator("WordsInBlacklistCounter")
                }
            }
        }
        longAccumulator
    }

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("AccumulatorsBroadcastWordCount").setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(5))
        ssc.sparkContext.setLogLevel("WARN")
        // ssc.checkpoint("hdfs://CentOS:8020/user/spark/checkpoint/AccumulatorsBroadcastWordCount")

        val lines = ssc.socketTextStream("CentOS", 9999, StorageLevel.MEMORY_AND_DISK_SER)
        // val wordsCnt = lines.flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _)
        val wordsCnt = lines.flatMap(_.split("\\s+"))

        wordsCnt.foreachRDD((rdd: RDD[(String)], time: Time) => {
            println(s"==================================${time}==========================** " + rdd.getNumPartitions)
            // 定时更新broadcast
            if ((time.milliseconds / 10000) % 5 == 0) {
                BroadcastWrapperObject.update(rdd.sparkContext)
            }
            // 获取黑名单广播变量， 用于数据量比较小，才广播
            // var blackList = AccumulatorsBroadcastWordCount.getBroadcastInstance(rdd.sparkContext)
            val wordBlackList = BroadcastWrapperObject.getInstance(rdd.sparkContext)
            // 定义一个全局Long型累加器， 记录被滤掉的黑名单记录数
            val dropWordCount = AccumulatorsBroadcastWordCount.getLongAccumulatorInstance(rdd.sparkContext)
            println("broadcast : " + wordBlackList.value)
            rdd.foreachPartition(partition => {
                var newAddBlackWord = Seq[String]()
                val conn = JDBCPool.getInstance().borrowObject()
                partition.foreach(record => {
                    if (wordBlackList.value.contains(record)) {
                        dropWordCount.add(1)
                    } else if (Seq("x", "y", "z").contains(record)) {
                        newAddBlackWord = newAddBlackWord :+ record
                    } else {
                        print(record)
                    }
                })
                if (newAddBlackWord.size > 0) {
                    import scala.collection.JavaConverters._
                    DML.insertBlackListData(conn, newAddBlackWord.asJava)
                }
            })

            //            val counts = rdd.filter(x => {
            //                if (wordBlackList.contains(x)) {
            //                    dropWordCount.add(1)
            //                    false
            //                } else if (Seq("x", "y", "z").contains(x)) {
            //                    // 这个是更新 broadcast 的示例
            //                    newWordBlackList = newWordBlackList :+ x
            //                    println(newWordBlackList)
            //                    false
            //                } else {
            //                    true
            //                }
            //            })

            // 使用累加器的过程中只能使用一次action的操作才能保证结果的准确性
            // 因为spark是延迟执行的， 有一个长长的任务链， 遇到action算子才会执行
            // 如下， collect()和count()都是action算子， 调用这两个方法， 会重复执行两次这个任务链， 也就是执行两次filter()方法
            // 所以， 累加器执行了两次， 导致下面两条打印的值不一致
            // 解决办法就是缓存上面的filter()的结果集， 这样就切断任务链前面的依赖关系，使之不要从头在执行一次即可
            // 如下缓存
            // counts.persist()

            //            val wordStr1 = counts.collect().mkString("[", ", ", "]")
            println(s"共滤掉黑名单记录数 : ${dropWordCount.value}")

            // val wordStr2 = counts.count()
            // println(s"共滤掉黑名单记录数 : ${dropWordCount.value}")
        })
        //        wordsCnt.print()

        ssc.start()
        ssc.awaitTermination()
    }
}