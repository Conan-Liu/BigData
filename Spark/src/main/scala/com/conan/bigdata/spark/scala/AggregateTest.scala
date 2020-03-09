package com.conan.bigdata.spark.scala

/**
  * 实现 "1,2,3" 这种字符串的数据，按位置相加，返回字符串
  * 如: "1,2,3" + "1,2,5" + "5,6,1" = "7,10,9"
  */
object AggregateTest {

    def testAggregate1(): Unit = {
        val seq = Seq(("a1", ("A", "1,2,3")), ("a2", ("A", "1,2,3")), ("a3", ("A", "1,2,3")), ("a4", ("A", "1,2,3")))
        def getAddition(fist: String, second: String): String = {
            val s1 = fist.split("\\,")
            val s2 = second.split("\\,")
            val resSeq = for (i <- 0 until s1.length) yield (s1(i).toInt + s2(i).toInt).toString
            resSeq.mkString(",")
        }
        val res = seq.aggregate[String]("0,0,0")((x, y) => getAddition(x, y._2._2), (x, y) => getAddition(x, y))
        println(res)
    }

    def testAggregate2(): Unit = {
        val seq = Seq(("a1", ("A", "1,2,3")), ("a2", ("A", "1,2,3")), ("a3", ("A", "1,2,3")), ("a4", ("A", "1,2,3")))
        def getReduce(first: String, second: String): String = {
            val s1 = first.split(",")
            val s2 = second.split(",")
            // for yield 用法， 返回一个列表
            val resSeq = for (i <- 0 until s1.length) yield s1(i).toInt + s2(i).toInt
            resSeq.mkString(",")
        }

        val res = seq.map(_._2._2).reduce((x, y) => getReduce(x, y))
        println(res)
    }

    def main(args: Array[String]): Unit = {
        testAggregate1()
        testAggregate2()
    }

}