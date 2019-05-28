package com.conan.bigdata.spark.scala

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2019/5/25.
  *
  * 使用较好的序列化方式， 虽然增加 CPU 的使用， 但是明显降低数据量， 利于集群内部网络传输和存储
  */
case class LongPackageTuple(a: String, b: Int)

object FileWithKryoWordCount {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("FileWithKryoWordCount")
            .setMaster("local[4]")
            // .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // 使用Kryo 来序列化
            .registerKryoClasses(Array(classOf[String], classOf[LongPackageTuple])) // 这个方法默认覆盖两个 Kryo 序列化的参数， 使用了这个方法可以不用写上面那一行的配置， 看源码可知
        val sc = new SparkContext(sparkConf)

        val lines = sc.textFile("file:\\D:\\Tools\\spark\\dataworks.sql")
        val words = lines.flatMap(_.split(" "))
        // 使用默认序列化格式
        // 就算指定Kryo来序列化和注册String类， 内存占用也不减少， 说明spark已经对基本数据类型String默认使用了Kryo来序列化， 也注册了该类， 所以，这种情况下，就不需要特别指定了
        words.persist(StorageLevel.MEMORY_ONLY_SER)

        val wordsCnt1 = words.map(x => (x, 1))
        // 序列化后，占用内存大幅减少，不过需要额外占用cpu来实现序列化
        // 使用Kryo来序列化， 更加减少了内存占用, 这是Tuple类型的，注册和不注册占用空间大小一致， 可能内部底层，默认给java.lang和scala包注册过了
        wordsCnt1.persist(StorageLevel.MEMORY_ONLY_SER)

        // 这个是自己定义的bean类， 也就是数据类型, 这个用Kryo序列化反而大些， 因为这个类的路径太长了，
        // 所以这个时候， 一定要注册， 这样内存占用能明显减小
        val wordsCnt2 = words.map(x => LongPackageTuple(x, 1))
        wordsCnt2.persist(StorageLevel.MEMORY_ONLY_SER)

        println(wordsCnt1.count())
        println(wordsCnt2.count())

        // 这里暂停的目的是让程序停在这里， 这样可以查看web页面， 不然一会就执行完了， web看不到
        while (true) {
            Thread.sleep(5000)
        }

        sc.stop()
    }
}