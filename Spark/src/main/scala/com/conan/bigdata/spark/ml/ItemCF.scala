package com.conan.bigdata.spark.ml

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * 参考 https://www.jianshu.com/p/27b1c035b693
  *
  * 样例数据如下
  * User
  * U1  LiuYi
  * U2  ChenEr
  * U3  ZhangSan
  * U4  LiSi
  *
  * Item
  * I1  1001
  * I2  1002
  * I3  1003
  * I4  1004
  * I5  1005
  *
  * Rating
  * U1  I1  3
  * U1  I2  3
  * U1  I3  4
  * U1  I4  4
  * U1  I5  5
  * U2  I1  4
  * U3  I1  3
  * U3  I3  5
  * U3  I4  3
  * U4  I1  3
  * U4  I2  4
  * U4  I3  5
  *
  * 则物品偏好矩阵，数字则为用户对该物品的偏好程度
  * *    U1  U2  U3  U4
  * I1   3   4   3   3
  * I2   3   0   0   4
  * I3   4   0   5   5
  * I4   4   0   3   0
  * I5   5   0   0   0
  */
object ItemCF extends SparkVariable {

    case class ItemCFUser(user_id: String, user_name: String)

    case class ItemCFItem(item_id: String, item_name: String)

    case class ItemCFRating(user_id: String, item_id: String, rating: Double)

    /**
      * 2.相似度计算
      * 这是核心，相似度计算算法有很多，这里取 余弦相似度
      *
      * 同一用户物品消费交叉情况,因为不是所有的物品都会被用户同时购买，不需要考虑所有物品的交叉，只需要考虑用户贡献的物品交叉计算相似即可，减少计算量
      */
    def calculateCosSimilarity(ratingRDD: RDD[ItemCFRating]): RDD[(String, (String, Double))] = {
        // 物品之间的相似度，一个用户同时购买两个物品，那么这两个物品的贡献记为 1，两个用户则记为 2，计算余弦相似度的分子
        val userItemsRDD = ratingRDD.map(x => (x.user_id, x.item_id)).groupByKey().flatMap(x => {
            val list = new ListBuffer[((String, String), Int)]()
            for (i <- x._2; j <- x._2) {
                if (i != j)
                    list.append(((i, j), 1))
            }
            list
        }).reduceByKey(_ + _)
        // 每个物品被多少人消费，计算余弦相似度公式的分母
        val itemUsersRDD = ratingRDD.map(x => (x.item_id, 1)).reduceByKey(_ + _).collect().toMap
        val bc = sc.broadcast(itemUsersRDD)
        val simi = userItemsRDD.mapPartitions(x => {
            val list = new ListBuffer[((String, String), Int, Int, Int)]()
            val v = bc.value
            x.foreach(i => {
                val x = v.getOrElse(i._1._1, 0)
                val y = v.getOrElse(i._1._2, 0)
                list.append((i._1, i._2, x, y))
            })
            list.toIterator
        }).filter(x => x._2 > 0 && x._3 > 0 && x._4 > 0).map(x => (x._1._1, (x._1._2, x._2 / math.sqrt(x._3 * x._4))))

        //        userItemsRDD.foreach(println)
        //        itemUsersRDD.foreach(println)
        simi.foreach(println)
        simi
    }

    /**
      * 3.针对某个用户推荐
      * 找到某个用户之前消费的物品，在相似度矩阵中找出这些物品的相似物品，然后根据消费物品的评分*相似物品的相似度=用户对相似物品的感兴趣程度
      * 相当于对相似物品加权计算用户的感兴趣程度
      * 某个用户之前消费的多个物品，很可能会与同一个物品相似，那么这个物品的相似度应该相加，提升这个物品相似度
      * 按相似度倒序，推荐TopN即可
      */
    def recommendItemsToUser(ratingRDD: RDD[ItemCFRating], simi: RDD[(String, (String, Double))]): RDD[(String, Double)] = {
        val userItems = ratingRDD.filter(_.user_id == "U4").map(x => (x.item_id, x.rating))
        val userSimiItem = simi.join(userItems).map(x => (x._2._1._1, x._2._1._2 * x._2._2)).leftOuterJoin(userItems).filter(_._2._2.isEmpty).map(x => (x._1, x._2._1)).reduceByKey(_ + _).sortBy(_._2, false)
        println(userSimiItem.first)
        println("*******************************")
        userSimiItem.foreach(println)
        userSimiItem
    }

    /**
      * 4.针对多个用户推荐
      */
    def recommendItemsToUsers(): Unit = {

    }

    def main(args: Array[String]): Unit = {

        /**
          * 1.数据准备，生成用户，物品和评分的RDD
          * userRDD 用户RDD，字段(user_id, ...)， 注： ... 表示其它基本信息，如名称
          * itemRDD 物品RDD，字段(item_id, ...)
          * ratingRDD 评分RDD，字段(user_id,item_id,rating)
          */
        val userRDD = sc.parallelize(Seq[(String, String)](("U1", "LiuYi"), ("U2", "ChenEr"), ("U3", "ZhangSan"), ("U4", "LiSi"))).map(x => ItemCFUser(x._1, x._2))
        val itemRDD = sc.parallelize(Seq[(String, String)](("I1", "1001"), ("I2", "1002"), ("I3", "1003"), ("I4", "1004"), ("I4", "1004"))).map(x => ItemCFItem(x._1, x._2))
        val ratingRDD = sc.parallelize(Array(("U1", "I1", 3), ("U1", "I2", 3), ("U1", "I3", 4), ("U1", "I4", 4), ("U1", "I5", 5), ("U2", "I1", 4), ("U3", "I1", 3), ("U3", "I3", 5), ("U3", "I4", 3), ("U4", "I1", 3), ("U4", "I2", 4), ("U4", "I3", 5))).map(x => ItemCFRating(x._1, x._2, x._3))

        // 2.数据处理，生成用户物品相似度矩阵
        val simiRDD = calculateCosSimilarity(ratingRDD)

        println("**************************************")
        // 3.为指定用户推荐物品
        val recommItems = recommendItemsToUser(ratingRDD, simiRDD)

        sc.stop()
    }
}