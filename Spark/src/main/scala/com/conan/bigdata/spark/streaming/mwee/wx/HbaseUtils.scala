package com.conan.bigdata.spark.streaming.mwee.wx

import java.io.IOException
import java.util.concurrent.{ExecutorService, Executors}

import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

/**
  * create 'wx_user_tag',{NAME => 'f1',COMPRESSION => 'SNAPPY',BLOOMFILTER => 'NONE'},SPLITS => ['5']
  */
object HbaseUtils {

    def lpad(userId: String, wxFlag: Int): String = {
        val length = userId.length
        var newUserId: String = null
        length match {
            case 1 =>
                newUserId = "000000000" + userId
            case 2 =>
                newUserId = "00000000" + userId
            case 3 =>
                newUserId = "0000000" + userId
            case 4 =>
                newUserId = "000000" + userId
            case 5 =>
                newUserId = "00000" + userId
            case 6 =>
                newUserId = "0000" + userId
            case 7 =>
                newUserId = "000" + userId
            case 8 =>
                newUserId = "00" + userId
            case 9 =>
                newUserId = "0" + userId
            case 10 =>
                newUserId = userId
            case _ =>
                newUserId = ""
        }
        var stringBuilder: StringBuilder = new StringBuilder(newUserId)
        if (wxFlag == 1) {
            stringBuilder = stringBuilder.reverse.append("_1")
        }
        else {
            stringBuilder = stringBuilder.reverse.append("_2")
        }
        stringBuilder.toString
    }

    @volatile private var hbaseConf: Configuration = _

    def getHbaseConf: Configuration = {
        val properties = Tools.properties
        if (hbaseConf == null) {
            hbaseConf = HBaseConfiguration.create
            // 必须指定， 否则会报 schema 不一致
            hbaseConf.set("fs.defaultFS", "hdfs://nameservice1/")
            hbaseConf.set("dfs.nameservices", "nameservice1")
            hbaseConf.set("dfs.ha.namenodes.nameservice1", "nn1,nn2")
            hbaseConf.set("dfs.namenode.rpc-address.nameservice1.nn1", properties.getProperty("namenode.nn1"))
            hbaseConf.set("dfs.namenode.rpc-address.nameservice1.nn2", properties.getProperty("namenode.nn2"))
            hbaseConf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
            hbaseConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
            hbaseConf.set("hbase.zookeeper.quorum", properties.getProperty("zookeeper.list"))
            hbaseConf.set("hbase.zookeeper.property.clientPort", properties.getProperty("zookeeper.port"))
        }
        hbaseConf
    }

    @volatile private var connection: Connection = _

    def getHbaseConnection: Connection = {
        if (connection == null) {
            classOf[Connection] synchronized {
                if (connection == null) {
                    try {
                        val executorService: ExecutorService = Executors.newFixedThreadPool(5)
                        connection = ConnectionFactory.createConnection(getHbaseConf, executorService)
                    }
                    catch {
                        case e: Exception =>
                            e.printStackTrace()
                    }
                }
            }
        }
        connection
    }

    def wxGetCity(e: JSONObject, wxFlag: Int): JSONObject = {
        val conn = getHbaseConnection
        val table = conn.getTable(TableName.valueOf(Constant.HBASE_TABLE_NAME))
        val get = new Get(Bytes.toBytes(lpad(String.valueOf(e.getIntValue("userId")), wxFlag)))
        val result = table.get(get)
        val list = result.listCells
        if (list == null) {
            e.put("city", "")
            return e
        }
        for (cell <- list.asScala) {
            val city = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
            e.put("city", city)
        }
        e
    }

    def bulkPut(listArgs: ListBuffer[String]) {
        val conn = getHbaseConnection
        conn.isClosed
        val table = conn.getTable(TableName.valueOf(Constant.HBASE_TABLE_NAME))
        val list = new java.util.ArrayList[Put](1024)
        var put: Put = null
        for (l <- listArgs) {
            val ss = l.split(",")
            put = new Put(Bytes.toBytes(ss(0)))
            // 设置WAL的持久化级别，Mutation的子类都可以设置该级别，HLog是WAL的实现
            put.setDurability(Durability.SKIP_WAL)
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(ss(1)))
            list.add(put)
        }
        table.put(list)
        // 关闭table表链接，这个操作不会关闭hbase connection链接
        table.close()
        // 不要关闭hbase connection链接，这里使用线程池，关闭后就没了
        // conn.close()
    }

    // scala没有check异常，这里相当于java throws
    @throws[IOException]
    def majorCompact() {
        val conn: Connection = getHbaseConnection
        val admin: Admin = conn.getAdmin
        admin.flush(TableName.valueOf(Constant.HBASE_TABLE_NAME))
        admin.majorCompact(TableName.valueOf(Constant.HBASE_TABLE_NAME), Bytes.toBytes("f1"))
        // 不要关闭conn
        // conn.close()
    }
}
