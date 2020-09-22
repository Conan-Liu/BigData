package com.conan.bigdata.spark.sql

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * 窗口函数和分析函数的演示
  * 分析函数在有order by时需要注意
  * preceding：用于累加前N行（分区之内）。若是从分区第一行头开始，则为 unbounded。 N为：相对当前行向前的偏移量
  * following ：与preceding相反，累加后N行（分区之内）。若是累加到该分区结束，则为 unbounded。N为：相对当前行向后的偏移量
  * current row：顾名思义，当前行，偏移量为0
  * 说明：上边的前N，后M，以及current row均会累加该偏移量所在行
  * 默认 rows between unbounded preceding and current row
  */
object OverFunctionExp extends SparkVariable {

    case class ProductUse(product_code: Int, event_date: String, duration: Int)

    def main(args: Array[String]): Unit = {
        val rdd = sc.parallelize(Seq[ProductUse](
            ProductUse(1438, "2016-05-13", 165),
            ProductUse(1438, "2016-05-14", 595),
            ProductUse(1438, "2016-05-15", 105),
            ProductUse(1629, "2016-05-13", 12340),
            ProductUse(1629, "2016-05-14", 13850),
            ProductUse(1629, "2016-05-15", 227)
        ))

        import spark.implicits._
        val ds: Dataset[ProductUse] = spark.createDataset(rdd)
        ds.createOrReplaceTempView("product_use")

        // 直接分组求和
        val p11DF = spark.sql(
            """
              |select
              |product_code,
              |event_date,
              |sum(duration) over(partition by product_code) as sum_duration
              |from product_use
            """.stripMargin)
        p11DF.show(false)

        // sql实现，注意在使用sum等分析函数时，需要注意order by带来数据顺序上的问题
        // 我们已经有当天用户的使用时长，我们期望在进行统计的时候，14号能累加13号的，15号能累加14、13号的，以此类推累加历史所有
        val p12DF = spark.sql(
            """
              |select
              |product_code,
              |event_date,
              |sum(duration) over(partition by product_code order by event_date asc) as sum_duration
              |from product_use
            """.stripMargin)
        p12DF.show(false)
        val p13DF = spark.sql(
            """
              |select
              |product_code,
              |event_date,
              |sum(duration) over(partition by product_code order by event_date asc rows between unbounded preceding and current row) as sum_duration
              |from product_use
            """.stripMargin)
        p13DF.show(false)
        // 累加前1天
        val p14DF = spark.sql(
            """
              |select
              |product_code,
              |event_date,
              |sum(duration) over(partition by product_code order by event_date asc rows between 1 preceding and current row) as sum_duration
              |from product_use
            """.stripMargin)
        p14DF.show(false)
        // 累加前一天，后一天
        val p15DF = spark.sql(
            """
              |select
              |product_code,
              |event_date,
              |sum(duration) over(partition by product_code order by event_date asc rows between 1 preceding and 1 following) as sum_duration
              |from product_use
            """.stripMargin)
        p15DF.show(false)
        // 累计该分区内所有行，与不写order by效果一样
        val p16DF = spark.sql(
            """
              |select
              |product_code,
              |event_date,
              |sum(duration) over(partition by product_code order by event_date asc rows between unbounded preceding and unbounded following) as sum_duration
              |from product_use
            """.stripMargin)
        p16DF.show(false)

        // dataframe实现
        val p21Win = Window.partitionBy("product_code")
        val p22Win = p21Win.orderBy($"event_date".asc)
        val p23Win = p21Win.rowsBetween(Long.MinValue, 0)
        val p24Win = p21Win.rowsBetween(-1, 0)
        val p25Win = p21Win.rowsBetween(-1, 1)
        val p26Win = p21Win.rowsBetween(Long.MinValue, Long.MaxValue)
        val p21DF = ds.select(
            $"product_code",
            $"event_date",
            sum($"duration").over(p21Win).as("sum_duration")
        )
        p21DF.show(false)


        // 产品按照时长大小排名
        val p3Win = Window.partitionBy("product_code").orderBy($"duration".desc)
        val p3DF = ds.select(
            $"product_code",
            $"event_date",
            row_number().over(p3Win).as("rn")
        )
        p3DF.show(false)


        // 上卷rollup操作
        val rollDF=ds.rollup($"product_code",$"event_date").agg(sum($"duration").as("sum"),max($"duration").as("max"),avg($"duration").as("avg")).orderBy($"product_code".asc,$"event_date".asc)
        rollDF.show(false)

    }
}
