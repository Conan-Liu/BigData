package com.conan.bigdata.spark.streaming.mwee.buriedpoint.service

import com.alibaba.fastjson.JSONObject
import com.conan.bigdata.spark.streaming.mwee.buriedpoint.common.{BuriedPointData, Constant}

/**
  * Created by Administrator on 2019/5/5.
  */
object CheckService {

    /**
      * 数据校验
      */
        def verify(jsonObj: JSONObject): (Boolean, String) = {
            val timestamp = jsonObj.getLong(BuriedPointData.TIMESTAMP)
            if (timestamp > System.currentTimeMillis()) {
                return (false, s"timestamp=$timestamp 大于当前时间")
            }

            // 验证数据类型
            val jsonObjVerify = verifyDataType(jsonObj)
            if (!jsonObjVerify._1) {
                return jsonObjVerify
            }

            // biz对象
            val bizObj = jsonObj.getJSONObject(BuriedPointData.BIZ)
            if (bizObj != null) {
                val bizVerify = verifyDataType(bizObj)
                if (!bizVerify._1) {
                    return bizVerify
                }
            }
            (true, null)
        }

    /**
      * 数据类型检查
      *
      * @param jsonObj
      * @return
      */
    def verifyDataType(jsonObj: JSONObject): (Boolean, String) = {
        // 集合 set 隐式转换, 两种转换方式，挑一种
        import scala.collection.JavaConverters._
        for (key <- jsonObj.keySet().asScala) {
            val typeOption: Option[String] = BuriedPointData.DATA_TYPE.get(key)
            if (typeOption.nonEmpty) {
                val simpleName: String = jsonObj.get(key).getClass.getSimpleName
                // 浮点型特殊处理
                if (typeOption.get == Constant.DOUBLE) {
                    if (simpleName != Constant.DOUBLE && simpleName != Constant.INTEGER && simpleName != Constant.LONG && simpleName != Constant.BIGDECIMAL) {
                        return (false, s"key=$key, type=$simpleName 数据类型和文档中的不一致")
                    }
                }
                // 其它类型处理
                else if (typeOption.get != simpleName) {
                    return (false, s"key=$key, type=$simpleName 数据类型和文档中的不一致")
                }
            }
        }
        (true, null)
    }
}