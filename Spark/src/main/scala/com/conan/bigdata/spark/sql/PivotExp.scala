package com.conan.bigdata.spark.sql

import com.conan.bigdata.spark.utils.SparkVariable

/**
  * Spark SQL行转列，列转行演示
  */
object PivotExp extends SparkVariable {

    case class Scores(course: String, name: String, score: Int)

    def main(args: Array[String]): Unit = {

        import spark.implicits._
        val rdd = sc.parallelize(Seq[Scores](
            Scores("数学", "张三", 88),
            Scores("语文", "张三", 92),
            Scores("英语", "张三", 77),
            Scores("数学", "王五", 65),
            Scores("语文", "王五", 87),
            Scores("英语", "王五", 90),
            Scores("数学", "李雷", 67),
            Scores("语文", "李雷", 33),
            Scores("英语", "李雷", 24),
            Scores("数学", "宫九", 77),
            Scores("语文", "宫九", 87),
            Scores("英语", "宫九", 90)
        ))
        val ds = spark.createDataset(rdd)
        ds.createOrReplaceTempView("scores")
        ds.show(false)

        // 行转列，需要注意的是，从spark2.4开始，才支持sql语句pivot语法，如果版本不满足可以使用table api的方式实现
        // val sql1 = spark.sql(
        //    """
        //      |select
        //      |*
        //      |from scores
        //      |pivot (sum(score) for name in ('张三','王五','李雷','宫九'))
        //    """.stripMargin)
        // sql1.show(false)

        // table api实现pivot行转列
        val p1 = ds.groupBy("name").pivot("course", Seq[String]("数学", "语文", "英语")).sum("score")
        p1.createOrReplaceTempView("p1")
        p1.show(false)

        // 列转行
        val p2=spark.sql(
            """
              |select
              |a.name,
              |t1
              |from
              |(
              |select
              |*,concat('数学:',`数学`,',','语文:',`语文`,',','英语:',`英语`) as arr1
              |from p1
              |) a
              |lateral view explode(split(arr1,',')) as t1
            """.stripMargin)
        p2.show(false)
    }
}
