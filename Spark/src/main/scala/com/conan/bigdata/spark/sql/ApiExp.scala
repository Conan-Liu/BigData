package com.conan.bigdata.spark.sql

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

object ApiExp extends SparkVariable {

    // 方外面能正确执行，方法里面会报错
    case class Person(name: String, age: Int)

    def main(args: Array[String]): Unit = {

        // 减少中间RDD的产生
        val source: RDD[(String, Any)] = sc.parallelize(Seq[(String, Any)](("Michael", null), ("Andy", 30), ("Justin", 19), ("Liu", 30)))
        import spark.implicits._

        /**
          * DataFrame演示
          */
        // 编程接口的方式实现DataSet
        val structType: StructType = StructType(
            Seq(
                StructField("name", StringType),
                StructField("age", IntegerType)
            )
        )
        val sourceDF: DataFrame = spark.createDataFrame(source.map(p => Row(p._1, p._2)), structType) //.cache()
        sourceDF.printSchema()
        sourceDF.show(false)

        sourceDF.select("name").show(false)

        sourceDF.select($"name", $"age" + 1).show(false)

        // 注意null是无法参与比较的，所以不会显示，无论大于还是小于都不会显示
        // where功能和这样一样，内部就是调用filter
        sourceDF.filter(col("age") > 21).show(false)

        // 就算上面没有cache算子，查看DAG图，groupby算子这也有skip，默认cache？
        sourceDF.groupBy().count().show(false)
        sourceDF.groupBy($"age").count().show(false)

        sourceDF.createOrReplaceTempView("people")
        // spark.catalog.dropTempView("people")  // 删除视图
        spark.sql("select * from people").show(false)

        /**
          * DataSet演示
          */
        // 反射的方式创建DataSet
        val sDF: DataFrame = source.map(x => Person(x._1, x._2.asInstanceOf[Int])).toDF()
        sDF.show(false)
        val dataset: Dataset[Person] = sDF.as[Person]
        dataset.show(false)


        /**
          * UDF演示
          */
        spark.udf.register("myavg", MyAverageUDAF)
        val myavg: DataFrame = spark.sql("select myavg(age) as avg from people")
        myavg.show(false)

        spark.udf.register("myudf",)
    }

}
