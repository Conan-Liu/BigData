package com.conan.bigdata.spark.job

import java.security.MessageDigest
import java.util.UUID

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FsShell, Path}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
import org.apache.hadoop.io.{ArrayWritable, Writable}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/8/31.
  */
object UserTagDetailHBase {

    val JOB_NAME: String = "USER_ACTION_TO_HBASE"
    val TABLE_NAME: String = "user_action"
    val FAMILY_NAME: String = "info"
    val IN_PATH: String = "/user/hive/warehouse/dmd.db/user_tag_detail/{action_id=1,action_id=2,action_id=3,action_id=4,action_id=5,action_id=6,action_id=7,action_id=8}"
    //    val IN_PATH: String = "/user/hive/warehouse/dmd.db/user_tag_detail/action_id=4"
    val OUTPUT_PATH: String = "/user/hadoop/hbase/user_action"
    val EXT_LIBS: String = "/user/hadoop/libs"
    val fromDateFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val toDateFormat: FastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")

    def main(args: Array[String]): Unit = {
        val hbaseConf = HBaseConfiguration.create()
        // 必须指定， 否则会报 schema 不一致
        hbaseConf.set("fs.defaultFS", "hdfs://nameservice1/")
        hbaseConf.set("dfs.nameservices", "nameservice1")
        hbaseConf.set("dfs.ha.namenodes.nameservice1", "nn1,nn2")
        hbaseConf.set("dfs.namenode.rpc-address.nameservice1.nn1", "nn1.hadoop.pdbd.prod:8020")
        hbaseConf.set("dfs.namenode.rpc-address.nameservice1.nn2", "nn2.hadoop.pdbd.prod:8020")
        hbaseConf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
        hbaseConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
        hbaseConf.set("mapreduce.reduce.memory.mb", "4096")
        hbaseConf.set("mapreduce.reduce.shuffle.input.buffer.percent", "0.3")
        hbaseConf.set("mapreduce.reduce.shuffle.parallelcopies", "3")
        hbaseConf.set("mapreduce.map.maxattempts", "2")
        hbaseConf.set("hbase.zookeeper.quorum", "10.1.39.98,10.1.39.99,10.1.39.100")
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
        hbaseConf.set("hbase.client.retries.number", "2")
        hbaseConf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", "1024");

        deleteOutputPath(hbaseConf)

        val sparkConf = new SparkConf().setAppName("UserTagDetailHBase")
        val sc = new SparkContext(sparkConf)

        //        userTagDetail.take(10).foreach {
        //            case (k, v) =>
        //                val writables = v.get()
        //                val id = writables(0)
        //                val mw_id = writables(1)
        //                println(writables.length + "  " + id + " " + mw_id)
        //        }
        lazy val job = Job.getInstance(hbaseConf)
        job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setMapOutputValueClass(classOf[KeyValue])

        val connection = ConnectionFactory.createConnection(hbaseConf)
        val tableName = TableName.valueOf(TABLE_NAME)
        val table = connection.getTable(tableName)
        //        val hbaseAdmin = connection.getAdmin.asInstanceOf[HBaseAdmin]
        //        hbaseAdmin.disableTable(tableName)
        //        hbaseAdmin.truncateTable(tableName, true)

        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(tableName))

        val userTagDetail = sc.hadoopFile(IN_PATH, classOf[MapredParquetInputFormat], classOf[Void], classOf[ArrayWritable], 10)
        //                val userTagDetail = sc.newAPIHadoopFile(IN_PATH, classOf[MapredParquetInputFormat], classOf[Void], classOf[ArrayWritable], hbaseConf)

        val hbaseRdd = userTagDetail.map(x => {
            val v = x._2.get()
            val rowkey = createRowKey(String.valueOf(v(2)), String.valueOf(v(6)))
            val userAction = createValue(v)
            (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("user_action"), Bytes.toBytes(userAction)))
        }).sortBy(kv => kv._1, true)

        //        val userTagDetail = sc.parallelize(Array((Bytes.toBytes("1000"), Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("test"), Bytes.toBytes("foo1"))))
        //
        //        val hbaseRDD = userTagDetail.map(x => {
        //            (new ImmutableBytesWritable(x._1), new KeyValue(x._1, x._2, x._3, x._4))
        //        })

        hbaseRdd.saveAsNewAPIHadoopFile(OUTPUT_PATH, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], hbaseConf)

        val fs = new FsShell(hbaseConf)
        fs.run(Array("-chmod", "-R", "777", OUTPUT_PATH))

        val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
        bulkLoader.doBulkLoad(new Path(OUTPUT_PATH), table.asInstanceOf[HTable])

        //        hbaseAdmin.enableTable(tableName)

        println("Yes Yes Yes! ...........................................")
    }

    def deleteOutputPath(conf: Configuration): Unit = {
        val fs = FileSystem.get(conf)
        val out = new Path(OUTPUT_PATH)
        if (fs.exists(out)) {
            fs.delete(out, true)
        }
    }

    def createRowKey(mwId: String, busiTime: String): String = {
        val fullMwId = new StringBuffer(lpad(mwId)).reverse().toString
        var toMD5 = ""
        try {
            val md: MessageDigest = MessageDigest.getInstance("MD5")
            md.update(UUID.randomUUID.toString.getBytes)
            val b: Array[Byte] = md.digest
            var i: Int = 0
            val buf: StringBuffer = new StringBuffer("")
            var offset: Int = 0
            while (offset < b.length) {
                {
                    i = b(offset)
                    if (i < 0) i += 256
                    if (i < 16) buf.append("0")
                    buf.append(Integer.toHexString(i))
                }
                {
                    offset += 1;
                    offset - 1
                }
            }
            toMD5 = buf.toString.substring(0, 4) + buf.toString.substring(28, 32)

        } catch {
            case e: Exception => {
                e.printStackTrace()
            }
        }

        fullMwId + toDateFormat.format(fromDateFormat.parse(busiTime)) + toMD5
    }

    def createValue(writable: Array[Writable]): String = {
        val sb = new StringBuffer()
        for (w <- writable) {
            sb.append("|").append(String.valueOf(w))
        }
        sb.toString.substring(1)
    }

    def lpad(str: String): String = {
        var fullStr = ""
        str.length match {
            case 1 => fullStr = "000000000" + str
            case 2 => fullStr = "00000000" + str
            case 3 => fullStr = "0000000" + str
            case 4 => fullStr = "000000" + str
            case 5 => fullStr = "00000" + str
            case 6 => fullStr = "0000" + str
            case 7 => fullStr = "000" + str
            case 8 => fullStr = "00" + str
            case 9 => fullStr = "0" + str
            case 10 => fullStr = str
            case _ => fullStr = "9999999999"
        }
        fullStr
    }
}