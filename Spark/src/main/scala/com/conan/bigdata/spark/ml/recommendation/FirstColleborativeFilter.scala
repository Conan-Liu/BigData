package com.conan.bigdata.spark.ml.recommendation

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.functions._

/**
  */

object FirstColleborativeFilter extends SparkVariable{

    case class Customer(id: Int, name: String, address: String, nation: String, phone: String, mktSegment: String, comment: String)

    case class Order(id: Int, customer: String, status: String, totalPrice: Double, date: String, priority: String, clerk: String, shipPriority: Double, comment: String)

    case class LineItem(orders: Int, part: Int)

    def main(args: Array[String]): Unit = {

        import spark.implicits._
        val customerDF = spark.sparkContext.textFile("").map(_.split("")).map(x => Customer(x(0).toInt, x(1), x(2), x(3), x(4), x(5), x(6))).toDF()
        customerDF.createOrReplaceTempView("customer")
        val orderDF = spark.sparkContext.textFile("").map(_.split("")).map(x => Order(x(0).toInt, x(1), x(2), x(3).toDouble, x(4), x(5), x(6), x(7).toDouble, x(8))).toDF()
        orderDF.createOrReplaceTempView("order")

        // 查询用户订单信息
        val customerPartDF = spark.sql(
            """
              |SELECT
              |c.id customer,
              |i.part part
              |FROM customer c,orders o,itemline i
              |WHERE c.id=o.customer and o.id=i.orders
            """.stripMargin)

        // 默认没有评分，需要增加默认评分 10
        val resultDF = customerPartDF.withColumn("rating", col("id") * 0 + 10.0)

        // 生成测试训练集, 该函数根据weights权重，将一个RDD切分成多个RDD, 权重和为 1
        val Array(traing, test) = resultDF.randomSplit(Array(0.8, 0.2))
        val als=new ALS().setMaxIter(1)
            .setUserCol("customer")
            .setItemCol("part")
            .setRatingCol("rating")
            .setRegParam(0.01)
        val model=als.fit(traing)

        // 得出测试集的推荐结果
        val predictions=model.transform(test)
        predictions.show(false)

        spark.stop()
    }
}