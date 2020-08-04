package com.conan.bigdata.spark.streaming.mwee.wx

import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.utils.ZKGroupTopicDirs
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.example.ExampleInputFormat
import org.apache.parquet.io.InvalidRecordException
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * 美味公众号群发消息互动
  *
  * kafka
  * /usr/local/kafka_2.10-0.10.0.1/bin/kafka-topics.sh --create --zookeeper 10.1.24.159:2181,10.1.24.160:2181,10.1.24.161:2181 --replication-factor 2 --partitions 6 --topic wxinteractive
  *
  * spark2-submit --class com.mwee.bigdata.spark.streaming.wx.App \
  * --master yarn \
  * --deploy-mode cluster \
  * --driver-memory 3g \
  * --executor-memory 2g \
  * --executor-cores 2 \
  * --num-executors 2 \
  * /bdata/task/sparkstreaming/spark-110-SNAPSHOT.jar
  */
object App {

    private val log: Logger = LoggerFactory.getLogger("App")

    def main(args: Array[String]): Unit = {
        val batchDuration = Minutes(3)
        val sparkConf = new SparkConf()
        // 目前是每个批次执行的结果往mysql中写，mysql将会是性能瓶颈
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "20")
        sparkConf.set("spark.dynamicAllocation.enabled", "false")
        // sparkConf.registerKryoClasses(Array(classOf[JSONObject]))
        val sc = new SparkContext(sparkConf)
        sc.setLogLevel("WARN")
        val ssc = new StreamingContext(sc, batchDuration)
        // ssc.addStreamingListener(new WxAppListener(ssc.sparkContext.getConf.get("spark.app.name", "WxApp")))

        val properties = Tools.properties
        val kafkaParams = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> properties.getProperty("kafka.broker.list"),
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.GROUP_ID_CONFIG -> properties.getProperty("kafka.consumer.groupid"),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest", // latest   earliest
            // java 数据类型
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
            //ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> (300000:java.lang.Integer),
            //ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG -> (400000:java.lang.Integer)
        )

        val topics = Set(properties.getProperty("kafka.topic"))

        val map = Map(new TopicPartition(properties.getProperty("kafka.topic"), 0) -> 10L)

        val assign = ConsumerStrategies.Assign[String, String](map.keys, kafkaParams, map)

        val sourceStream = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            // 这里offset没有存储在第三方，就没使用assign模式，直接使用subscribe模式
            // ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
            new OptimizerSubscribe[String, String](
                new java.util.ArrayList(topics.asJavaCollection),
                new java.util.HashMap[String, Object](kafkaParams.asJava),
                java.util.Collections.emptyMap[TopicPartition, java.lang.Long]())
//            assign
        )

        hbaseBulkPut(sc)

        // 首次启动，时间间隔可能没有一天，需要特殊处理
        var isFirstStart = true
        // 定时器
        sourceStream.foreachRDD((rdd, time) => {
            // 每天5点开始
            if ("09".equals(Tools.getSchedulerHour(time.milliseconds))) {
                val acc = Tools.getAccInstance(rdd.sparkContext)
                if (isFirstStart || (time.milliseconds - acc.value) >= Constant.SCHEDULER_TIME) {
                    isFirstStart = false
                    hbaseBulkPut(rdd.sparkContext)
                    acc.reset()
                    acc.add(time.milliseconds)
                }
            }
        }
        )

        var offsetRanges = Array[OffsetRange]()
        val filterStream = sourceStream.transform((rdd, time) => {
            // 简易定时
            //            if ("05:38".equals(Tools.getSchedulerMin(time.milliseconds))) {
            //                // 更新hbase
            //                hbaseBulkPut(ssc.sparkContext)
            //            }

            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        })
                // 日志转json
                .map(x => {
            try {
                val json = JSON.parseObject(x.value())
                (json.getIntValue("userId"), json)
            } catch {
                case _: Exception => (0, JSON.parseObject("{}"))
            }
        })
                // 过滤无效的数据，只保留发消息的日志
                .filter(x => {
            val msgType = x._2.getString("msgType")
            // 所有上报的日志都用来更新最新的互动时间
            x._1 > 0 // && ("text".equals(msgType) || ("event".equals(msgType) && "unsubscribe".equals(x._2.getString("event"))))
        })

        val stream = filterStream
                // 汇总取一条最新的日志数据做最近访问时间
                .groupByKey()
                .mapValues(x => {
                    var maxJson: JSONObject = null
                    x.foreach(xx => {
                        if (maxJson == null || maxJson.getLongValue("lastJoinTime") < xx.getLongValue("lastJoinTime")) {
                            maxJson = xx
                        }
                    })
                    // 测试环境取关后status不变，就为了修正这个
                    if ("event".equals(maxJson.getString("msgType")) && "unsubscribe".equals(maxJson.getString("event")))
                        maxJson.put("status", "0")
                    maxJson
                })
                // 大一大二的地理位置比较操蛋，非要取GPS地址的城市，城市号就是取本身对应的城市
                .mapPartitions(p => {
            val mapCity = Tools.getCity
            val properties = Tools.properties
            val table = HbaseUtils.getHbaseTable
            p.map(x => {
                val json = x._2
                val appId = json.getString("appId")
                // 目前大一未上线
                if ("dayi".equals(appId)) {
                    HbaseUtils.wxGetCity(table, json, 1)
                    // 大二和测试
                } else if (properties.getProperty("app.id").equals(appId)) {
                    HbaseUtils.wxGetCity(table, json, 2)
                } else {
                    // 城市号获取城市
                    json.put("city", mapCity.getOrElse(appId, "0")) // 生产
                    // json.put("city", mapCity.getOrElse(appId, "258")) // 测试没有城市号，给个默认值
                    json
                }
            })
        }
        )

        DBs.setup()
        stream.foreachRDD(rdd => {

            // 数据量目前较小
            // 采用Driver端删除插入的方式，插入速度受影响
            val collect = rdd.collect()
            if (collect.length > 0) {
                DB.autoCommit(implicit session => {
                    SQL("delete from wx_join_user_track_tmp").update().apply()
                })
                DB.localTx(implicit session => {
                    val sql = SQL("insert into wx_join_user_track_tmp(user_id,user_tag,city,create_time,mobile,open_id,sex,status,last_join_time,app_id) values(?,?,?,?,?,?,?,?,?,?)")
                    for (x <- collect) {
                        sql.bind(x.getIntValue("userId"), "", x.getString("city"),
                            x.getLongValue("createTime"), x.getString("mobile"), x.getString("openId"), x.getString("sex"), x.getString("status"),
                            x.getLongValue("lastJoinTime"), x.getString("appId")
                        ).update().apply()
                    }
                })

                // 这里采用删除插入的方式，自增的id会增长到超过数据类型的范围，需要优化的点
                DB.autoCommit(implicit session => {
                    SQL("DELETE t1 FROM wx_join_user_track t1,wx_join_user_track_tmp t2 WHERE t1.user_id=t2.user_id").update().apply()
                    SQL("insert into wx_join_user_track(user_id,user_tag,city,create_time,mobile,open_id,sex,status,last_join_time,app_id) select user_id,user_tag,city,create_time,mobile,open_id,sex,status,last_join_time,app_id from wx_join_user_track_tmp where status = '1'").update().apply()
                })
            }

            // offset的管理留待后续完善
            sourceStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

        })

        ssc.start()
        ssc.awaitTermination()

    }

    def hbaseBulkPut(sc: SparkContext): Unit = {
        val path = "/user/hive/warehouse/dw.db/{assoc_wx_user_track_tmp1,assoc_user_tag_new_tmp1}/000000_0"
        val conf = HbaseUtils.getHbaseConf
        val exits = FileSystem.get(conf).exists(new Path(path))
        if (exits) {
            val job = sc.newAPIHadoopFile(path, classOf[ExampleInputFormat], classOf[Void], classOf[Group], conf)
            job.foreachPartition(p => {
                val list = new ListBuffer[String]
                var userId: String = null
                var city: String = null
                val properties = Tools.properties
                val table = HbaseUtils.getHbaseTable
                p.foreach(x => {
                    val arr = x._2
                    userId = String.valueOf(arr.getLong("user_id", 0))
                    city = arr.getString("city", 0)
                    try {
                        // 大二号的parquet文件比大一号多一个字段
                        val appId = arr.getString("app_id", 0)
                        if (properties.getProperty("app.id").equals(appId)) {
                            userId = HbaseUtils.lpad(userId, 2)
                            list += (userId + "," + city)
                        }
                    } catch {
                        case _: InvalidRecordException =>
                            userId = HbaseUtils.lpad(userId, 1)
                            list += (userId + "," + city)
                    }
                    if (list.size >= 2000) {
                        HbaseUtils.bulkPut(table, list)
                        list.clear()
                    }
                })
                HbaseUtils.bulkPut(table, list)
                list.clear()
            })
            HbaseUtils.majorCompact()
            log.warn("Hbase Table [{}] update successfully !!!", Constant.HBASE_TABLE_NAME)
        } else {
            log.warn("Hbase Table [{}] update error !!!, because of no file in path", Constant.HBASE_TABLE_NAME)
        }
    }

    def persistOffsets(offsets: Array[OffsetRange], groupId: String, storeEndOffset: Boolean, zkUtils: ZkUtils): Unit = {
        offsets.foreach(o => {
            val zKGroupTopicDirs = new ZKGroupTopicDirs(groupId, o.topic)
        })
    }

}
