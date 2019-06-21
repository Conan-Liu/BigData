package com.conan.bigdata.spark.scala

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by Conan on 2019/5/14.
  * PageRank是执行多次连接的一个迭代算法, 因此它是RDD分区操作的一个很好的用例.
  * 算法会维护两个数据集:
  * 一个由(pageID，linkList)的元素组成, 包含每个页面的相邻页面的列表
  * 另一个由(pageID，rank)元素组成, 包含每个页面的当前排序值, 它按如下步骤进行计算。
  * *
  * 将每个页面的排序值初始化为 1.0
  * 在每次迭代中, 对页面p, 向其每个相邻页面(有直接链接的页面)发送一个值为 rank(p)/numNeighbors(p) 的贡献值.
  * 将每个页面的排序值设为 0.15 + 0.85 * contributionsReceived
  *
  * 最后两个步骤会重复几个循环，在此过程中, 算法会逐渐收敛于每个页面的实际PageRank值.
  * 在实际操作中，收敛通常需要大约10轮迭代.
  */
object PageRank {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("PageRank").setMaster("local[1]")
        val sc = new SparkContext(sparkConf)

        val alpha = 0.85
        // 每一轮迭代计算page的rank值， 10次迭代基本可以收敛
        val iterCnt = 10

        val links = sc.parallelize(
            Seq(
                ("A", Seq("A", "C", "D")),
                ("B", Seq("D")),
                ("C", Seq("B", "D")),
                ("D", Seq())
            )
        ).partitionBy(new HashPartitioner(2)).persist()

        var ranks = links.mapValues(_ => 1.0)

        for (i <- 0 until iterCnt) {
            val contributions = links.join(ranks).flatMap(c => {
                val linkList = c._2._1
                val rank = c._2._2
                linkList.map(dest => (dest, rank / linkList.size))
            })

            ranks = contributions.reduceByKey((x, y) => x + y).mapValues(x => {
                (1 - alpha) + alpha * x
            })
        }

        ranks.sortByKey().foreach(println)
    }
}