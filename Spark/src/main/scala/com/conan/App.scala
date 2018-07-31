package com.conan

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile("")

    lines.repartition(12)

    lines.countByValue()

    val words = lines.flatMap(x => x.split("\\s+")).map(x => (x,1))

    words.lookup("")
  }
}
