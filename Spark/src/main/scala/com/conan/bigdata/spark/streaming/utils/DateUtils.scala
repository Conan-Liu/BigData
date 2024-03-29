package com.conan.bigdata.spark.streaming.utils

import org.apache.commons.lang3.time.FastDateFormat

/**
  */
object DateUtils {

    val IN_TIME_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val OUT_TIME_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")

    def getTime(time: String) = {
        IN_TIME_FORMAT
    }

    def parseToMinute(time: String): String = {
        OUT_TIME_FORMAT.format(getTime(time))
    }

    def main(args: Array[String]): Unit = {
        println(parseToMinute("2019-05-02 21:50:01"))

    }
}