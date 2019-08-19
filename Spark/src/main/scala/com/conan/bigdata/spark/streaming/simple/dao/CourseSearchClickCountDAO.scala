package com.conan.bigdata.spark.streaming.simple.dao

import com.conan.bigdata.spark.streaming.simple.bean.{CourseClickCount, CourseSearchClickCount}
import com.conan.bigdata.spark.utils.hbase.HbaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  */
object CourseSearchClickCountDAO {

    val HBASE_NAME = "course_search_clickcount"
    val COLUMN_FAMILY = "info"
    val COUNT_QUALIFER = "click_count"

    /**
      * 保存数据到HBase
      *
      * @param list
      */
    def saveToHbase(list: ListBuffer[CourseSearchClickCount]): Unit = {
        val table = HbaseUtils.getInstance().getTable(HBASE_NAME)

        for (ele <- list) {
            // 这个 API 就可以直接完成累加的操作， 注意
            table.incrementColumnValue(Bytes.toBytes(ele.daySearchCourse),
                Bytes.toBytes(COLUMN_FAMILY),
                Bytes.toBytes(COUNT_QUALIFER),
                ele.clickCount
            )
        }
    }

    /**
      * 根据 Rowkey 查询数据
      *
      * @param daySearchCourse
      * @return
      */
    def count(daySearchCourse: String): Long = {
        val table = HbaseUtils.getInstance().getTable(HBASE_NAME)

        val get = new Get(Bytes.toBytes(daySearchCourse))
        val value = table.get(get).getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COUNT_QUALIFER))

        if (value == null) {
            0l
        } else {
            Bytes.toLong(value)
        }
    }

    def main(args: Array[String]): Unit = {
        val list = new ListBuffer[CourseSearchClickCount]

        list.append(CourseSearchClickCount("20171111_baidu_8", 80))
        list.append(CourseSearchClickCount("20171111_sogou_9", 90))
        list.append(CourseSearchClickCount("20171111_bing_1", 100))

        saveToHbase(list)

        // 190 : 169 : 164 : 176 : 182 : 181
        println(count("20171111_baidu_8") + " : " + count("20171111_sogou_9") + " : " + count("20171111_bing_1") + " : " + count("20190503_131") + " : " + count("20190503_145") + " : " + count("20190503_146"))
    }
}