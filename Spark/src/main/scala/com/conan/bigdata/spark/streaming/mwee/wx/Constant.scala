package com.conan.bigdata.spark.streaming.mwee.wx

object Constant {

    val HBASE_TABLE_NAME: String = "wx_user_tag"

    val APP_ENV: String = Tools.properties.getProperty("app.env")

    // 定时器，每隔这么长时间更新一次大一大二地理位置，毫秒
    val SCHEDULER_TIME = 86400000
}
