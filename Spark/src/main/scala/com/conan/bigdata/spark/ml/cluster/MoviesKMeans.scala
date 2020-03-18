package com.conan.bigdata.spark.ml.cluster

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.mllib.clustering.KMeans

/**
  */
object MoviesKMeans extends SparkVariable {

    val ITEM_PATH = "E:\\BigData\\Spark\\ml\\ml-100k\\u.item"
    val GENRE_PATH = "E:\\BigData\\Spark\\ml\\ml-100k\\u.genre"
    val DATA_PATH = "E:\\BigData\\Spark\\ml\\ml-100k\\u.data"

    def main(args: Array[String]): Unit = {

        val movies = sc.textFile(ITEM_PATH)
        val genres = sc.textFile(GENRE_PATH)
        val data = sc.textFile(DATA_PATH)

        val genreMap = genres.map(x => {
            val ss = x.split("\\|")
            (ss(1), ss(0))
        }).collectAsMap()
        println(genreMap)

        val moviesAndGenres = movies.map(_.split("\\|")).map(x => {
            val genre = x.toSeq.slice(5, x.length)
            val genreAssigned = genre.zipWithIndex.filter(_._1 == "1").map(x=>genreMap.get(x._2.toString))
            (x(0).toInt, (x(1), genreAssigned))
        })
        println(moviesAndGenres.first())

        val K=5
        val iterations=10
        val numRuns=3
        val kmeansModel=KMeans.train(null,K,iterations,numRuns)
    }
}