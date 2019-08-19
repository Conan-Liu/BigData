package com.conan.bigdata.spark.streaming.flownotice.util

import java.text.DecimalFormat

import redis.clients.jedis.Jedis

/**
  *
  * redis 里面有一个hash表， 每个月一份， 记录每个月对应手机号的所有流量值
  * 键值对分别是:  K V
  * 例子：
  * FlowNoticeTitle201901    一月份hash表
  *
  * FhoneTitle13852293070    hash表里面的一个字段， 就是一个键值对的 K
  * 2019.01                  这是 V , 也就是该手机号码对应的流量值
  */
object RedisClient extends Serializable {

    @transient private val flowNoticeTitle = "FlowNoticeTitle"
    @transient private val etKey = "FlowNoticeEtKey"
    @transient private val phoneTitle = "FhoneTitle"
    @transient private val flowLimitStage6: Float = 6 * 1024
    @transient private val flowLimitStage8: Float = 8 * 1024
    @transient private val flowLimitStage10: Float = 10 * 1024

    def hincrBy(fields: List[(String, String, Float)]): (List[(String)],List[(String,String,String)]) = {
        var jedis: Jedis = null
        val df = new DecimalFormat("######0.00")
        var resultList:List[(String)]=List[(String)]()
        var flowList:List[(String,String,String)]=List[(String,String,String)]()
        try {
            jedis = RedisClusterClient.getRedisClient()
            var key = ""
            var allFlows: java.util.Map[String, String] = null
            for (field <- fields) {
                key = field._1.substring(0, 6) //
                if (true) {
                    allFlows = jedis.hgetAll(flowNoticeTitle + key)
                    println("current months " + key + " size is: " + allFlows.size())

                    val ext = jedis.get(etKey)
                    if (ext == null) {

                    }
                }

                val dateStr = field._1
                val phone = field._2
                val value = field._3

                var res: Double = 0
                var flow: Double = 0
                val result = allFlows.get(phoneTitle + phone)
                if (result == null || result == "" || result == "0") {
                    flow = 0
                } else {
                    flow = result.toDouble
                }
                res = jedis.hincrByFloat(flowNoticeTitle + key, phoneTitle + phone, value)
                val ftres = df.format(res)
                flowList = flowList:::List((phone,dateStr,ftres.toString))
                // 流量限流
                if (res > flowLimitStage6 && res < flowLimitStage8 && flow <= flowLimitStage6) {
                    resultList = resultList ::: List("00001" + "|" + phone + "|" + ftres.toString + "," + dateStr + "|" + "00")
                }else if(res>flowLimitStage8&&res<flowLimitStage10&&flow<flowLimitStage8){
                    resultList = resultList:::List("00002"+"|"+phone+"|"+ftres.toString+","+dateStr+"|"+"00")
                }else if(res>flowLimitStage10 && flow<flowLimitStage10){
                    resultList = resultList:::List("00003"+"|"+phone+"|"+ftres.toString+","+dateStr+"|"+"00")
                }
            }
        }catch{
            case e:Exception=>println(e.getMessage)
        }finally {
            if(jedis!=null)
                jedis.close()
        }
        (resultList,flowList)
    }


}