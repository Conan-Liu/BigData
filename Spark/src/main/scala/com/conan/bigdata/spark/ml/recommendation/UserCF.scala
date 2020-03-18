package com.conan.bigdata.spark.ml.recommendation

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * 参考 https://blog.csdn.net/qq_24908345/article/details/80407634
  *
  * 样例数据如下
  * User
  * U1  A
  * U2  B
  * U3  C
  * U4  D
  *
  * Item
  * I1  a
  * I2  b
  * I3  c
  * I4  d
  * I5  e
  *
  * Rating 参考如下用户物品的评分矩阵
  * *   I1  I2  I3  I4  I5
  * U1   2   1   0   3   0
  * U2   1   0   3   0   0
  * U3   0   5   0   0   1
  * U4   5   0   4   1   2
  */
object UserCF extends SparkVariable {

    case class UserCFRating(user_id: String, item_id: String, rating: Double)

    /**
      * 利用余弦相似度计算用户之间的相似度
      *
      * 很多用户相互之间并没有对同样物品产生过行为，当然这里要另外考虑一些生活必需品，这是很广泛的，不具有个性化的特点，不利于计算用户相似度
      * 所以可以转换下，先计算有多少用户消费过同一物品，那么就应该加入相似度计算，减少计算量
      */
    def calculateCosSimilarity(ratings: RDD[UserCFRating]): RDD[(String, (String, Double))] = {
        // 贡献度， 用户买一个物品贡献度为1，两个物品则为2，计算分子
        val fenzi = ratings.map(x => (x.item_id, x.user_id)).groupByKey().flatMap(x => {
            val list = new ListBuffer[((String, String), Int)]()
            for (i <- x._2; j <- x._2) {
                if (!i.equalsIgnoreCase(j)) {
                    list.append(((i, j), 1))
                }
            }
            list
        }).reduceByKey(_ + _)
        // 每个用户消费了多少物品，计算分母
        val fenmu = ratings.map(x => (x.user_id, 1)).reduceByKey(_ + _)
        val simi=fenzi.map(x=>(x._1._1,(x._1._2,x._2))).join(fenmu).map(x=>(x._2._1._1,(x._1,x._2._1._1,x._2._1._2,x._2._2))).join(fenmu)
            .map(x=>(x._2._1._1,x._2._1._2,x._2._1._3,x._2._1._4,x._2._2,x._2._1._3/math.sqrt(x._2._1._4*x._2._2))).map(x=>(x._1,(x._2,x._6)))

        simi.foreach(println)
        simi
    }

    def main(args: Array[String]): Unit = {

        val ratings = sc.parallelize[(String, String, Double)](Seq(("U1", "I1", 2.0), ("U1", "I2", 1.0), ("U1", "I4", 3.0), ("U2", "I1", 1.0), ("U2", "I3", 3.0), ("U3", "I2", 5.0), ("U3", "I5", 1.0), ("U4", "I1", 5.0), ("U4", "I3", 4.0), ("U4", "I4", 1.0), ("U4", "I5", 2.0))).map(x => UserCFRating(x._1, x._2, x._3))

        // 计算用户相似度
        val simi=calculateCosSimilarity(ratings)

        // 给用户推荐物品，未实现

    }

}