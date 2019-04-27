package com.conan.bigdata.spark.streaming

import java.sql.{Connection, DriverManager}
import redis.clients.jedis.Jedis
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Conan on 2019/4/27.
  * 词频统计， 写入mysql
  */
object WordCountToMysql {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setAppName("UpdateStateWordCount").setMaster("local[2]")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.sparkContext.setLogLevel("WARN")


        val lines = ssc.socketTextStream("CentOS", 9999)
        val result = lines.flatMap(x => x.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _)

        // 要有print操作， 否则，控制台不能打印出如下日志
        // -------------------------------------------
        // Time: 1556357605000 ms
        // -------------------------------------------
        result.print()

        result.foreachRDD(rdd => {
            println("该RDD的partition数: " + rdd.getNumPartitions)
            rdd.foreachPartition(partitionOfRecords => {
                val conn = createMysqlConnection()
                val jedis = createJedisConnection()
                partitionOfRecords.foreach(record => {
                    // mysql 不能保证同一个 word 合并计数值
                    val sql = "insert into word_count(word,word_cnt) values('%s',%s)".format(record._1, record._2)
                    conn.createStatement().execute(sql)

                    // 使用redis ， 来合并同一个word的计数
                    jedis.incrBy(record._1, record._2)
                })
            })
        })

        ssc.start
        ssc.awaitTermination
    }

    def createMysqlConnection(): Connection = {
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "123")
    }

    // 这个是写在foreachPartition里面的， 所以该RDD有几个partition就执行几次
    def createJedisConnection(): Jedis = {
        val jedis = new Jedis("CentOS", 6379)
        println(jedis.ping())
        jedis
    }
}