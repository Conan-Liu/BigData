package com.conan.bigdata.spark.sparksql

import com.conan.bigdata.spark.utils.SparkVariable

/**
  * RDD，DF和DS的区别
  */
object DiffBetweenRDDAndDFDS extends SparkVariable {

    case class Student(gradeId: Int, sno: Int, name: String)

    def main(args: Array[String]): Unit = {
        val data = sc.parallelize[(Int, Int, String)](Seq((1, 1, "hadoop"), (1, 2, "hive"), (2, 3, "spark"), (2, 4, "flink"), (2, 5, "kafka"), (2, 6, "hbase"), (2, 7, "redis"), (3, 9, "zookeeper"), (3, 8, "xx")), 2)
                .map(x => Student(x._1, x._2, x._3))

        //import spark.implicits._
        val dataDF = spark.createDataFrame(data, classOf[Student])
        //println(dataDF.schema.size)

        val dataDS = spark.createDataset[Student](data)

        //dataDF.foreach(x => {
            // x.getAs[Int]("gradeId")
        //})

        //dataDS.foreach(x => {
           // x.name
        //})

        dataDF.show(10, false)
    }
}
