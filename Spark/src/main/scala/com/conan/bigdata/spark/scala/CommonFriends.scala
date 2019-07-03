package com.conan.bigdata.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算下面用户的共同好友， 冒号前是一个用户， 冒号后是该用户的好友
  * A:B,C,D,F,E,O
  * B:A,C,E,K
  * C:F,A,D,I
  * D:A,E,F,L
  * E:B,C,D,M,L
  * F:A,B,C,D,E,O,M
  * G:A,C,D,E,F
  * H:A,C,D,E,O
  * I:A,O
  * J:B,O
  * K:A,C,D
  * L:D,E,F
  * M:E,F,G
  * O:A,H,I,J
  */
object CommonFriends {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("CommonFriends").setMaster("local[2]")
        val sc = new SparkContext(sparkConf)
        // 准备数据
        val data = sc.parallelize(Seq("A:B,C,D,F,E,O",
            "B:A,C,E,K",
            "C:F,A,D,I",
            "D:A,E,F,L",
            "E:B,C,D,M,L",
            "F:A,B,C,D,E,O,M",
            "G:A,C,D,E,F",
            "H:A,C,D,E,O",
            "I:A,O",
            "J:B,O",
            "K:A,C,D",
            "L:D,E,F",
            "M:E,F,G",
            "O:A,H,I,J"), 3)

        val reserveFriend = data.map(x => {
            val xs = x.split(":")
            (xs(0), xs(1))
        }).flatMapValues(x => x.split(",")).map(x => (x._2, x._1)).groupByKey()

        val comFri = reserveFriend.flatMap(x => {
            val comFriUser = x._1
            val users = x._2.split(",")
            val twoUser = for (i <- 0 until users.length - 1)
                for (j <- i + 1 until users.length)
                    yield (users(i) + "-" + users(j), comFriUser)
            twoUser
        })
        reserveFriend.sortByKey().map(x => x._1 + ":" + x._2).foreach(println)
    }
}