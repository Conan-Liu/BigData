package com.conan.bigdata.spark.streaming

/**
  * MacBook的命令
  * nc -l 9999
  */
object OnlineBlackListFilter extends StreamingVariable {
    def main(args: Array[String]) {

        // 模拟黑名单列表， 这里简单用数组表示
        val blackList = Array(("hadoop", true), ("mahout", true))
        val blackListRDD = ssc.sparkContext.parallelize(blackList, 2)
        // blackListRDD.cache()
        // 读取日志数据的格式如下
        // time   name
        val clickStream = ssc.socketTextStream("localhost", 9999)

        val clickStreamFormatted = clickStream.map(x => (x.split("\\s+")(1), x))
        // transform 函数把这一个批次的数据当成RDD来操作，然后返回一个DStream，继续作为流处理
        clickStreamFormatted.transform(rdd => {
            val joinedBlackListRDD = rdd.leftOuterJoin(blackListRDD)
            // (key, (x, true))
            // joinedBlackListRDD.take(10).foreach(x => print(x + "\t"))
            val validClicked = joinedBlackListRDD.filter(joinedItem => {
                if (joinedItem._2._2.getOrElse(false))
                    false
                else
                    true
            })
            validClicked.map(valid => {
                valid._2._1
            })
        }).print()

        ssc.start()
        ssc.stop()
        ssc.awaitTermination()
    }
}


//Time: 1483498200000 ms
//Time: 1483498500000 ms
//Time: 1483498800000 ms
//Time: 1483499100000 ms