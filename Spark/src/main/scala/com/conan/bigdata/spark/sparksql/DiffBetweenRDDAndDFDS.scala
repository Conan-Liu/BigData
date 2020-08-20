package com.conan.bigdata.spark.sparksql

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

/**
  * RDD，DF和DS的区别
  */
object DiffBetweenRDDAndDFDS extends SparkVariable {

    case class Student(gradeId: Int, sno: Int, name: String)

    def main(args: Array[String]): Unit = {
        val data = sc.parallelize[(Int, Int, String)](Seq((1, 1, "hadoop"), (1, 2, "hive"), (2, 3, "spark"), (2, 4, "flink"), (2, 5, "kafka"), (2, 6, "hbase"), (2, 7, "redis"), (3, 9, "zookeeper"), (3, 8, "xx")), 2)
        val dataStudent = data.map(x => Student(x._1, x._2, x._3))
        val dataRow = data.map(x => Row(x._1, x._2, x._3))

        import spark.implicits._
        // 编程接口创建DataFrame
        val schema = StructType(
            Seq(
                StructField("gradeId", DataTypes.IntegerType, false),
                StructField("sno", DataTypes.IntegerType, true),
                StructField("name", DataTypes.StringType, true)
            )
        )
        // 这种写法不知道为什么不行
        // val dataDF = spark.createDataFrame(dataStudent, classOf[Student])
        val dataDF = spark.createDataFrame(dataRow, schema)

        // RDD内的数据是元组，转DataFrame，字段比较少的时候可以选这个
        val dataDFTuple = data.toDF("gradeId", "sno", "name")

        implicit val mapEncoder=Encoders.kryo[Student]
        // 使用case class创建Dataset
        val dataDS = spark.createDataset[Student](dataStudent)

        // RDD转Dataset
        val dataDS1 = dataStudent.toDS()

        // DataFrame转Dataset
        val dataDS2 = dataDFTuple.as[Student]

        // Dataset转DataFrame
        val dataDF2 = dataDS1.toDF()

        // DataFrame，Dataset转RDD
        dataDF.rdd
        dataDS.rdd

        dataDFTuple.foreach(x => {
            println(x.getAs[Int]("gradeId"))
        })

        dataDS.foreach(x => {
            println(x.name)
        })

        dataDS.show(10, false)
        dataDF.show(10, false)

        spark.stop()
    }
}
