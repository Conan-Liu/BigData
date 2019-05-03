package com.conan.bigdata.spark.streaming.simple.dao

import com.conan.bigdata.spark.streaming.simple.bean.CourseClickCount
import com.conan.bigdata.spark.utils.hbase.HbaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * Created by Conan on 2019/5/3.
  */
object CourseClickCountDAO {

    val HBASE_NAME = "course_clickcount"
    val COLUMN_FAMILY = "info"
    val COUNT_QUALIFER = "click_count"

    /**
      * 保存数据到HBase
      *
      * @param list
      */
    def saveToHbase(list: ListBuffer[CourseClickCount]): Unit = {
        val table = HbaseUtils.getInstance().getTable(HBASE_NAME)

        for (ele <- list) {
            // 这个 API 就可以直接完成累加的操作， 注意
            table.incrementColumnValue(Bytes.toBytes(ele.dayCourseid),
                Bytes.toBytes(COLUMN_FAMILY),
                Bytes.toBytes(COUNT_QUALIFER),
                ele.clickCount
            )
        }
    }

    /**
      * 根据 Rowkey 查询数据
      *
      * @param dayCourse
      * @return
      */
    def count(dayCourse: String): Long = {
        val table = HbaseUtils.getInstance().getTable(HBASE_NAME)

        val get = new Get(Bytes.toBytes(dayCourse))
        val value = table.get(get).getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COUNT_QUALIFER))

        if (value == null) {
            0l
        } else {
            Bytes.toLong(value)
        }
    }

    def main(args: Array[String]): Unit = {
        val list = new ListBuffer[CourseClickCount]

        list.append(CourseClickCount("20171111_8", 8))
        list.append(CourseClickCount("20171111_9", 9))
        list.append(CourseClickCount("20171111_1", 100))

        saveToHbase(list)

        // 190 : 169 : 164 : 176 : 182 : 181
        println(count("20190503_112") + " : " + count("20190503_128") + " : " + count("20190503_130") + " : " + count("20190503_131") + " : " + count("20190503_145") + " : " + count("20190503_146"))
    }
}