package com.conan.bigdata.spark.job

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
import org.apache.hadoop.io.ArrayWritable
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/8/31.
  */
object UserTagDetailHBase {

    val IN_PATH = "/user/hive/warehouse/dmd.db/user_tag_detail/action_id=4"

    def main(args: Array[String]): Unit = {
        val hadoopConf=new Configuration()
        // 必须指定， 否则会报 schema 不一致
        hadoopConf.set("fs.defaultFS", "hdfs://nameservice1/")
        hadoopConf.set("dfs.nameservices", "nameservice1")
        hadoopConf.set("dfs.ha.namenodes.nameservice1", "nn1,nn2")
        hadoopConf.set("dfs.namenode.rpc-address.nameservice1.nn1", "nn1.hadoop.pdbd.prod:8020")
        hadoopConf.set("dfs.namenode.rpc-address.nameservice1.nn2", "nn2.hadoop.pdbd.prod:8020")
        hadoopConf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
        hadoopConf.set("mapreduce.reduce.memory.mb", "4096")
        hadoopConf.set("mapreduce.reduce.shuffle.input.buffer.percent", "0.3")
        hadoopConf.set("mapreduce.reduce.shuffle.parallelcopies", "3")
        hadoopConf.set("mapreduce.map.maxattempts", "2")
        hadoopConf.set("hbase.zookeeper.quorum", "10.1.39.98,10.1.39.99,10.1.39.100")
        hadoopConf.set("hbase.zookeeper.property.clientPort", "2181")
        val sparkConf = new SparkConf().setAppName("UserTagDetailHBase")
        val sc = new SparkContext(sparkConf)
//        val userTagDetail=sc.newAPIHadoopFile(IN_PATH,classOf[MapredParquetInputFormat],classOf[Void],classOf[ArrayWritable], hadoopConf)
        val userTagDetail = sc.hadoopFile(IN_PATH, classOf[MapredParquetInputFormat], classOf[Void], classOf[ArrayWritable])

        userTagDetail.take(10).foreach {
            case (k, v) =>
                val writables = v.get()
                val id = writables(0)
                val mw_id = writables(1)
                println(writables.length + "  " + id + " " + mw_id)
        }
    }
}