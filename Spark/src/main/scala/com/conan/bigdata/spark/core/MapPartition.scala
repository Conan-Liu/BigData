package com.conan.bigdata.spark.core

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
import org.apache.hadoop.io.ArrayWritable

import scala.collection.mutable.ArrayBuffer

/**
  * 将多份数据进行关联是数据处理过程中非常普遍的用法，不过在分布式计算系统中，这个问题往往会变的非常麻烦，
  * 因为框架提供的 join 操作一般会将所有数据根据 key 发送到所有的 reduce 分区中去，也就是 shuffle 的过程。
  * 造成大量的网络以及磁盘IO消耗，运行效率极其低下，这个过程一般被称为 reduce-side-join。
  * 如果其中有张表较小的话，我们则可以自己实现在 map 端实现数据关联，跳过大量数据进行 shuffle 的过程，运行时间得到大量缩短
  *
  * 数据样例
  * 小表： gender, gender_name     1 男， 2 女
  * 大表： user_id, gender         1234, 2   21,1
  *
  * 该类给出了mapPartitions的低效和高效的用法
  */
object MapPartition extends SparkVariable {

    def main(args: Array[String]): Unit = {

        // 小表最好是map结构，便于直接匹配
        val small = sc.parallelize(Array(("1", "男"), ("2", "女"))).collectAsMap()
        // 广播小表， 这样每个机器上都会有一份数据副本， 实现本地mapjoin
        val smallBC = sc.broadcast(small)

        // 读取用户信息
        val rdd = sc.hadoopFile[Void, ArrayWritable]("/user/hive/warehouse/dw.db/mw_user_info", classOf[MapredParquetInputFormat], classOf[Void], classOf[ArrayWritable], 4)
        val large = rdd.map(x => {
            val V = x._2.toStrings
            (V(0), V(1))
        })

        // 对大数据进行遍历，使用mapPartition而不是map，因为mapPartition是在每个partition中进行操作
        // 因此可以减少遍历时新建smallBC.value对象的空间消耗，同时匹配不到的数据也不会返回
        // 但是一个partition可能会有很多数据，计算复杂，可能会造成内存溢出，也可能频繁GC
        val res1 = large.mapPartitions(iter => {
            val smallMap = smallBC.value
            val arrayBuffer = ArrayBuffer[(String, String, String)]()
            iter.foreach { case (userId, gender) => {
                // += 相当于函数，这里写成 arrayBuffer .+= 更容易理解， 函数后面传参数自然需要括号括起来, 括号里面是一个三元组
                arrayBuffer += ((userId, smallMap.getOrElse(gender, ""), gender))
            }
            }
            arrayBuffer.iterator
        })

        // for的守卫机制也可以实现，这种可能会造成内存溢出
        val res2 = large.mapPartitions(iter => {
            val smallMap = smallBC.value
            for ((userId, gender) <- iter)
                yield (userId, smallMap.getOrElse(userId, ""), gender)
        })

        res1.take(100).foreach(println)

        val fs = FileSystem.get(sc.hadoopConfiguration)
        fs.delete(new Path("/backup/mapjoin/d1"),true)
        res1.saveAsTextFile("/backup/mapjoin/d2")


        // mapPartitions 可能会造成内存溢出加用起来不方便， 下面给出相对高效的用法
        // mapPartitions中套map
        val niubi1=large.mapPartitions(x=>{
            println("连接数据库或者是获取一个广播变量")
            val bc = smallBC.value
            val res=x.map(y=>{
                println("处理每一条数据")
                (y._1,bc.getOrElse(y._1,""),y._2)
            })
            // 可能无法关闭数据库等耗资源的链接，待验证
            println("关闭数据库链接")
            res
        })

        // 自定义迭代器，实现可以关闭耗资源的链接，无需缓存数据，就不会内存溢出
        val niubi2=large.mapPartitions(x=>{
            println("连接数据库或者其它耗资源的链接")
            val bc = smallBC.value
            // createDatabaseConnector()
            new Iterator[(String,String,String)]{
                // 判断是否还有数据，如果没有则关闭资源使用
                override def hasNext: Boolean ={
                    if(x.hasNext){
                        true
                    }else{
                        println("关闭数据库")
                        // closeDatabaseConnector()
                        false
                    }
                }

                override def next(): (String, String,String) ={
                    // 具体的数据处理逻辑，处理每一条数据
                    (x.next._1,bc.getOrElse(x.next._1,""),x.next._2)
                }
            }
        })
    }
}