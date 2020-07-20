package com.conan.bigdata.spark.streaming.mwee.wx

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.example.ExampleInputFormat
import org.apache.parquet.io.InvalidRecordException
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

import scala.collection.mutable.ListBuffer

object App {

    private val log: Logger = LoggerFactory.getLogger("App")

    def main(args: Array[String]): Unit = {
        val batchDuration = Seconds(20)
        val sparkConf = new SparkConf()
        val sc = new SparkContext(sparkConf)
        sc.setLogLevel("WARN")
        val ssc = new StreamingContext(sc, batchDuration)

        val properties = Tools.properties
        val kafkaParams = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> properties.getProperty("kafka.broker.list"),
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.GROUP_ID_CONFIG -> properties.getProperty("kafka.consumer.groupid"),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
            // java 数据类型
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
        )

        val topics = Set(properties.getProperty("kafka.topic"))

        val sourceStream = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
        )

        // 定时器
        sourceStream.foreachRDD((rdd, time) => {
            // 每天5点开始
            if ("05".equals(Tools.getSchedulerHour(time.milliseconds))) {
                val acc = Tools.getAccInstance(rdd.sparkContext)
                if ((time.milliseconds - acc.value) >= Constant.SCHEDULER_TIME) {
                    hbaseBulkPut(rdd.sparkContext)
                    acc.add(Constant.SCHEDULER_TIME)
                    log.warn("Hbase Table [{}] update successfully !!!", Constant.HBASE_TABLE_NAME)
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
                .filter(x => x._1 > 0 && "text".equals(x._2.getString("msgType")))

        // filterStream.print(5)

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
                    maxJson
                })
                // 大一大二的地理位置比较操蛋，非要取GPS地址的城市，城市号就是取本身对应的城市
                .mapPartitions { p => {
            val mapCity = Tools.getCity
            val properties = Tools.properties
            p.map(x => {
                val json = x._2
                val appId = json.getString("appId")
                // 目前大一未上线
                if ("dayi".equals(appId)) {
                    HbaseUtils.wxGetCity(json, 1)
                    // 大二和测试
                } else if (properties.getProperty("app.id").equals(appId)) {
                    HbaseUtils.wxGetCity(json, 2)
                } else {
                    // 城市号获取城市
                    json.put("city", mapCity.getOrElse(appId, ""))
                    json
                }
            })
        }
        }

        stream.foreachRDD(rdd => {
            // 数据量目前较小
            // 采用删除插入的方式
            val collect = rdd.collect()
            if (collect.length > 0) {
                DBs.setup()
                DB.autoCommit(implicit session => {
                    SQL("delete from wx_join_user_track_tmp").update().apply()
                })
                DB.localTx(implicit session => {
                    for (x <- collect) {
                        SQL("insert into wx_join_user_track_tmp(user_id,user_tag,city,create_time,mobile,open_id,sex,status,last_join_time,app_id) values(?,?,?,?,?,?,?,?,?,?)").bind(x.getIntValue("userId"), "", x.getString("city"),
                            x.getLongValue("createTime"), x.getString("mobile"), x.getString("openId"), x.getString("sex"), x.getString("status"),
                            x.getLongValue("lastJoinTime"), x.getString("appId")
                        ).update().apply()
                    }
                })

                DB.autoCommit(implicit session => {
                    SQL("delete from wx_join_user_track where user_id in (select user_id from wx_join_user_track_tmp)").update().apply()
                    SQL("insert into wx_join_user_track(user_id,user_tag,city,create_time,mobile,open_id,sex,status,last_join_time,app_id) select user_id,user_tag,city,create_time,mobile,open_id,sex,status,last_join_time,app_id from wx_join_user_track_tmp").update().apply()
                })
            }

            // offset的管理留待后续完善
            // transformStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

        })


        ssc.start()
        ssc.awaitTermination()
    }

    def hbaseBulkPut(sc: SparkContext): Unit = {
        val path = "/user/hive/warehouse/ods.db/assoc_user_tag_new_tmp1/*,/user/hive/warehouse/ods.db/assoc_wx_user_track_tmp1/*"
        //        val path = "/user/hive/warehouse/ods.db/assoc_wx_user_track/*"
        val job = sc.newAPIHadoopFile(path, classOf[ExampleInputFormat], classOf[Void], classOf[Group], HbaseUtils.getHbaseConf)
        job.foreachPartition(p => {
            val list = new ListBuffer[String]
            var userId: String = null
            var city: String = null
            val properties = Tools.properties
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
                if (list.size >= 1000) {
                    HbaseUtils.bulkPut(list)
                    list.clear()
                }
            })
            HbaseUtils.bulkPut(list)
        })
        HbaseUtils.majorCompact()
    }
}
