package com.conan.bigdata.spark.streaming.mwee.wx

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator
import org.slf4j.{Logger, LoggerFactory}
import scalikejdbc.config.DBs
import scala.collection.JavaConverters._

object Tools {

    private val log: Logger = LoggerFactory.getLogger("Tools")

    lazy val properties: Properties = getProjectProperties

    // 读取类路径下的配置文件
    def getProjectProperties: Properties = {
        val properties = new Properties()
        val in = Tools.getClass.getResourceAsStream("/application.properties")
        properties.load(in)
        log.warn("load properties successfully ...")
        val names = properties.stringPropertyNames().asScala
        for (name <- names) {
            println(name + " -> " + properties.getProperty(name))
        }
        properties
    }

    def getCity: Map[String, String] = {
        var cityMap: Map[String, String] = Map()
        // val appEnv = properties.getProperty("app.env")
        if ("dev".equalsIgnoreCase(Constant.APP_ENV)) {
            cityMap += ("wx8624eb15102147c6" -> "258") // 美味测试
        } else {
            cityMap += ("wx0fba8b5d617472f1" -> "19") // 北京
            cityMap += ("wx2b4d90784ddd9488" -> "139") // 武汉
            cityMap += ("wxbbef44ebc9443953" -> "170") // 南京
            cityMap += ("wx7a7cbe2fe444f58d" -> "46") // 广州
            cityMap += ("wx5d3b40652677833e" -> "339") // 宁波
            cityMap += ("wx898bfc317294e203" -> "258") // 上海
            cityMap += ("wxc79098fff055f973" -> "334") // 钱塘  杭州
            cityMap += ("wxdc352a6cee7645d1" -> "271") // 锦城  成都
            cityMap += ("wx5d8298788dae75e8" -> "172") // 苏州
            cityMap += ("wxc2da1bbab9b96480" -> "57") // 深圳
        }
        cityMap
    }


    private val dateFormat = new SimpleDateFormat("HH")

    def getSchedulerHour(time: Long): String = {
        val date = new Date(time)
        dateFormat.format(date)
    }

    // scala 实现volatile
    @volatile private var instance: LongAccumulator = _

    def getAccInstance(sc: SparkContext): LongAccumulator = {
        if (instance == null) {
            synchronized {
                if (instance == null) {
                    instance = sc.longAccumulator("timer")
                    instance.add(new Date().getTime)
                }
            }
        }
        instance
    }

    def main(args: Array[String]): Unit = {
        DBs.setup()
        DBs.readAsMap().foreach(println)
        println(getProjectProperties.getProperty("kafka.broker.list"))

        Class.forName("com.mysql.jdbc.Driver")
    }

}
