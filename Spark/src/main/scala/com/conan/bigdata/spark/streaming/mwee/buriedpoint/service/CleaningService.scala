package com.conan.bigdata.spark.streaming.mwee.buriedpoint.service

import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.conan.bigdata.spark.streaming.mwee.buriedpoint.common.{BuriedPointData, Constant}
import com.conan.bigdata.spark.streaming.mwee.buriedpoint.utils.KafkaProducerUtil
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.commons.lang3.time.DateFormatUtils
import org.slf4j.LoggerFactory

/**
  */
object CleaningService {

    private val LOG = LoggerFactory.getLogger(this.getClass)

    /**
      * 格式转换并保存到 sink  （数据异常不能影响offset提交）
      */
    def transformAndSave(message: String): Unit = {
        try {
            val jsonArr = JSON.parseArray(message)
            for (i <- 0 until jsonArr.size()) {
                val jsonObject = jsonArr.getJSONObject(i)
                // 数据验证， 验证未通过的数据丢弃
                val verifyResult = CheckService.verify(jsonObject)
                if (verifyResult._1) {
                    val line = transform(jsonObject)
                    KafkaProducerUtil.sendKafka(line)
                } else {
                    val timestamp = jsonObject.getLong(BuriedPointData.TIMESTAMP)
                    LOG.error(s"数据验证未通过 [${verifyResult._2}], timestamp=${timestamp}")
                }
            }
        } catch {
            case e: Exception => LOG.error(ExceptionUtils.getStackTrace(e), message)
        }
    }

    /**
      * 数据转换提取
      */
    def transform(jsonObj: JSONObject): String = {
        val timestamp = jsonObj.getLong(BuriedPointData.TIMESTAMP)
        val datetime = DateFormatUtils.format(new Date(timestamp), Constant.YYYYMMDD_HH_MM_SS)
        jsonObj.put(BuriedPointData.DATETIME, datetime)

        val bizJsonObj = jsonObj.getJSONObject(BuriedPointData.BIZ)
        if (bizJsonObj != null) {
            import scala.collection.JavaConverters._
            for (key <- bizJsonObj.keySet().asScala) {
                val fieldName = s"${BuriedPointData.BIZ}_${key}"
                val typeOption: Option[String] = BuriedPointData.DATA_TYPE.get(key)
                if (typeOption.nonEmpty) {
                    typeOption.get match {
                        // 类型匹配
                        case Constant.INTEGER => addNotNullValue(jsonObj, fieldName, bizJsonObj.getInteger(key))
                        case Constant.LONG => addNotNullValue(jsonObj, fieldName, bizJsonObj.getLong(key))
                        case Constant.STRING => addNotNullValue(jsonObj, fieldName, bizJsonObj.getString(key))
                        case Constant.DOUBLE => addNotNullValue(jsonObj, fieldName, bizJsonObj.getDouble(key))
                    }
                } else {
                    // DATA_TYPE不存在
                    addNotNullValue(jsonObj, fieldName, bizJsonObj.getString(key))
                }
            }
            jsonObj.put(BuriedPointData.BIZ, bizJsonObj.toJSONString)
        }

        transformDruid(jsonObj)

        jsonObj.toJSONString
    }

    /**
      * value 如果不为null， 就插入到 Event 中
      */
    def addNotNullValue(jsonObj: JSONObject, fieldName: String, value: Any): Unit = {
        if (value != null) {
            jsonObj.put(fieldName, value);
        }
    }

    /**
      * 转换成druid
      */
    def transformDruid(jsonObj: JSONObject): Unit = {
        addNotNullValue(jsonObj, BuriedPointData.PV, if ("Page" == jsonObj.getString(BuriedPointData.EVENT_TYPE)) 1 else 0)

        if ("mwapp" == jsonObj.getString(BuriedPointData.APP_NAME)) {
            addNotNullValue(jsonObj, BuriedPointData.UV_ID, jsonObj.getString(BuriedPointData.DISTINCT_ID))
        } else {
            val userId = jsonObj.getString(BuriedPointData.USERID)
            val mwId = jsonObj.getString(BuriedPointData.MWID)
            addNotNullValue(jsonObj, BuriedPointData.UV_ID, if (userId != null) userId else mwId)
        }
    }
}