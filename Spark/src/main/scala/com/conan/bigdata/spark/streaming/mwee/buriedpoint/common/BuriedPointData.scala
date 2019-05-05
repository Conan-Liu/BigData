package com.conan.bigdata.spark.streaming.mwee.buriedpoint.common

import com.conan.bigdata.spark.streaming.mwee.buriedpoint.common.Constant._
/**
  * Created by Administrator on 2019/5/5.
  */
object BuriedPointData {
    /**
      * 初始字段
      */
    val PLATFORM = "platform"
    val CONTAINER = "container"
    val APP_NAME = "app_name"
    val TIMESTAMP = "timestamp"
    val BIZ = "biz"
    val PAGE = "page"
    val DESC = "desc"
    val URL = "url"
    val BUILD = "build"
    val DISTINCT_ID= "distinct_id"
    val USERID="userid"
    val MWID="mwid"
    val EVENT_TYPE = "event_type"



    /**
      * 新增字段
      */
    val DATETIME = "datetime" //“20181217 12:05:01”
    //
    val PV = "pv"
    val UV_ID = "uv_id"


    /**
      * 埋点数据-数据类型
      */
    val DATA_TYPE: Map[String, String] = Map(
        PLATFORM -> STRING,
        CONTAINER -> STRING,
        APP_NAME -> STRING,
        "language" -> STRING,
        "language_app" -> STRING,
        TIMESTAMP -> LONG,
        "location" -> STRING,
        "city_id" -> STRING,
        "version" -> STRING,
        BUILD -> INTEGER,
        "channel" -> STRING,
        DISTINCT_ID -> STRING,
        "version_os" -> STRING,
        "sys_name" -> STRING,
        "model" -> STRING,
        "screen" -> STRING,
        "devicePixelRatio" -> DOUBLE,
        "net" -> STRING,
        "version_sdk" -> STRING,
        USERID -> INTEGER,
        "mwid" -> STRING,
        "phone" -> STRING,
        "path_app" -> STRING,
        "protocol" -> STRING,
        "domain" -> STRING,
        "port" -> STRING,
        "event" -> STRING,
        EVENT_TYPE -> STRING,
        "path_web" -> STRING,
        "hash" -> STRING,
        "referrer" -> STRING,
        "source" -> STRING,
        //biz对象：
        BIZ -> "JSONObject", //
        "shop_id" -> INTEGER,
        "brand_id" -> INTEGER,
        "index" -> INTEGER,
        "url" -> STRING,
        "content_id" -> STRING,
        "order_type" -> INTEGER,
        "order_no" -> STRING,
        PAGE -> STRING,
        "desc" -> STRING,
        "deviceId" -> STRING,
        "openId" -> STRING,
        "title" -> STRING
        //
    )
}