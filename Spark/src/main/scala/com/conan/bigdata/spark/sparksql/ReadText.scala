package com.conan.bigdata.spark.sparksql

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object ReadText extends SparkVariable{

    /**
      * 样例数据
      * 258|上海
      * 19|北京
      * 46|广州
      */
    case class City(city_id: String, city_name: String)

    def main(args: Array[String]): Unit = {

        val txtPath = "/user/hive/ext/aaa"

        // 下面有定义两种Schema的方法
        // 这种定义方法可以指定字段使用指定类型
        val schema_1 = StructType(
            List(
                StructField("city_id", StringType, true),
                StructField("city_name", StringType, true)
            )
        )
        // 下面这种定义字段的方法， 所有字段只能使用一种类型， 不灵活
        val colNames = Array[String]("city_id", "city_name")
        val schema_2 = StructType(colNames.map(fieldName => StructField(fieldName, StringType)))

        val txtDF = sc.textFile(txtPath)

        val rowRDD_1 = txtDF.map(_.split("\\|")).map(p => Row(p: _*)) // _* 把p这个case class转变成序列， 传入到可变长数组参数
        val rowRDD_2 = txtDF.map(_.split("\\|")).map(p => Row(p(0), p(1)))
        val data = spark.createDataFrame(rowRDD_2, schema_1)

        // spark2 的隐式转换
        // 在使用一些特殊的操作时，一定要加上 import spark.implicits._ 不然toDF、toDS无法使用
        //        import sparkSession.sqlContext.implicits._
        import spark.implicits._
        // 新版推荐
        val rowCase = txtDF.map(_.split("\\|")).map(p => City(p(0), p(1))).toDF()
        // 元组方式
        val rowCase1 = txtDF.map(line => {
            var ss = line.split("\\|")
            (ss(0), ss(1))
        }).toDF("col1", "col2")
        // DF 转成 DS
        val rowCase1DS = rowCase1.as[City]

        data.show(10)
        rowCase.show(10)

        // 释放资源
        spark.stop()
    }

}