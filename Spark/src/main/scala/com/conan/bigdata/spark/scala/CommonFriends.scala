package com.conan.bigdata.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算下面用户的共同好友， 冒号前是一个用户， 冒号后是该用户的好友
  * Hadoop 版本参考 com.conan.bigdata.hadoop.mr.CommonFriends
  */
object CommonFriends {

    def reverseFriendRelationShip(str: String): Array[(String, String)] = {
        val users = str.split(":")
        val user = users(0)
        val friends = users(1).split(",")
        for (fri <- friends)
            yield (fri, user)
    }

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

        // reverseFriend1 和 reverseFriend2 都能实现用户和好友的转换
        val reverseFriend1 = data.map(x => {
            val xs = x.split(":")
            (xs(0), xs(1))
        }).flatMapValues(x => x.split(",")).map(x => (x._2, x._1)).reduceByKey((x, y) => x + "," + y)
        reverseFriend1.foreach(println)

        val reverseFriend2 = data.flatMap(x => reverseFriendRelationShip(x)).reduceByKey((x, y) => x + "," + y)
        reverseFriend2.foreach(println)

        // flatMap 就是返回结果是集合， 或者数组， 然后给拍平
        val comFri = reverseFriend1.flatMap(x => {
            val comFriUser = x._1
            val users = x._2.split(",")

            /**
              * 正常来说，使用常规的两层for循环嵌套并没有什么问题，但是大多数嵌套循环的场景其实都是为了获取到内部元素，而且嵌套的for容易导致金字塔式的代码，所以scala的嵌套语法糖从可读性上来说，是值得使用的。多层嵌套使用;分开，内层的迭代元素可以同时取到外层循环的元素和内层循环的元素，生命周期和常规的嵌套for循环一致。
              * // 4. 嵌套for
              * for (file <- fileHere if file.isDirectory; lineFile <- file.listFiles())
              * println(lineFile.toString)
              * *
              * 改写成java大概就是这样：
              * for (File file : fileHere) {
              * if (file.isDirectory()) {
              * for (File lineFile : file.listFiles()) {
              *System.out.println(lineFile.toString());
              * }
              * }
              * }
              */
            // 这个两级for循环有点特殊, 如果想要多级循环里面的内容值带到外面， 则必须
            // 按照使用一个 for 关键字来实现多级循环， 如果是金字塔式的循环，则 yield 不起效果
            for (i <- 0 until users.length - 1; j <- i + 1 until users.length)
                yield (users(i) + "-" + users(j), comFriUser)
        })
        comFri.reduceByKey((x, y) => x + "," + y).mapValues(x => "[" + x + "]").foreach(println)
    }
}