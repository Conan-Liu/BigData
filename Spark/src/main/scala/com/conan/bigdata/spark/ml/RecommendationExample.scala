package com.conan.bigdata.spark.ml

import org.apache.spark.mllib.recommendation.ALS

/**
  * MLlib包下面的ALS实现推荐算法
  */
object RecommendationExample {

    def main(args: Array[String]): Unit = {
        ALS.train()
    }
}