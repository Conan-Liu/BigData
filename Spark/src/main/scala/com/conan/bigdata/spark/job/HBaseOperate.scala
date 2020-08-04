package com.conan.bigdata.spark.job

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.hadoop.hbase.client.{Mutation, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.example.ExampleInputFormat
import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer

/**
  * Spark操作HBase分两种
  * [[TableOutputFormat]] 直接输出表
  * [[HFileOutputFormat2]] 生成HFile文件，然后BulkLoad到指定表，适合大批量数据
  */
object HBaseOperate extends SparkVariable{

    def main(args: Array[String]): Unit = {

        val hbaseConf=sc.hadoopConfiguration
        // 必须指定， 否则会报 schema 不一致
        hbaseConf.set("fs.defaultFS", "hdfs://nameservice1/")
        hbaseConf.set("dfs.nameservices", "nameservice1")
        hbaseConf.set("dfs.ha.namenodes.nameservice1", "nn1,nn2")
        hbaseConf.set("dfs.namenode.rpc-address.nameservice1.nn1", "nn1.hadoop.pdbd.test.cn:8020")
        hbaseConf.set("dfs.namenode.rpc-address.nameservice1.nn2", "nn2.hadoop.pdbd.test.cn:8020")
        hbaseConf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
        hbaseConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
        hbaseConf.set("hbase.zookeeper.quorum", "zk1.hadoop.pdbd.test.cn,zk2.hadoop.pdbd.test.cn,zk3.hadoop.pdbd.test.cn")
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

        hbaseTableOutputFormat(sc)
        sc.stop()
    }

    /**
      * create 'table_test',{NAME => 'f1',BLOOMFILTER => 'NONE'}
      */
    def hbaseTableOutputFormat(sc: SparkContext): Unit = {
        val conf = sc.hadoopConfiguration
        conf.set(TableOutputFormat.OUTPUT_TABLE, "table_test")
        val job = Job.getInstance(conf, "jobName")
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Mutation])
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

        val data = sc.parallelize[String](Array[String]("1,jack,15", "2,lily,16", "3,mike,16", "4,conan,11"))

        val hbaseRdd = data.map(x => {
            val ss = x.split(",")
            val rowKey = Bytes.toBytes(ss(0))
            val put = new Put(rowKey)
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes(ss(1)))
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes(ss(2).toInt))
            (new ImmutableBytesWritable(rowKey), put)
        })


        hbaseRdd.saveAsNewAPIHadoopDataset(conf)
    }

    /**
      * 使用老版hadoopFile，需要配合老版的InputFormat
      * 使用新版newAPIHadoopFile，需要配合新版InputFormat
      */
    def hbaseBulkLoad(sc: SparkContext): Unit = {

        val job = sc.hadoopFile("", classOf[MapredParquetInputFormat], classOf[Void], classOf[ArrayWritable], 4)
        val job1 = sc.newAPIHadoopFile("", classOf[ExampleInputFormat], classOf[Void], classOf[Group], sc.hadoopConfiguration)

        job.foreachPartition(p => {
            val list = new ListBuffer[String]
            p.foreach(x => {
                val arr = x._2.get()
                list += (String.valueOf(arr(0)) + "," + String.valueOf(arr(3)))
            })

            // bulkPut(list)
        })
    }

}
