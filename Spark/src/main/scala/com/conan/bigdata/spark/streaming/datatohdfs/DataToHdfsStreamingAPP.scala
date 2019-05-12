package com.conan.bigdata.spark.streaming.datatohdfs

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path, PathFilter}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.{BZip2Codec, GzipCodec}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import spire.ClassTag

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Conan on 2019/5/11.
  */
object DataToHdfsStreamingAPP {

    private val listFiles: ArrayBuffer[Path] = ArrayBuffer()

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("DataToHdfsStreamingAPP").setMaster("local[*]")
            .set("spark.streaming.stopGracefullyOnShutdown", "true")
        val ssc = new StreamingContext(sparkConf, Seconds(10))
        val hadoopConf = ssc.sparkContext.hadoopConfiguration
        hadoopConf.set("fs.defaultFS", "hdfs://CentOS:8020")
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
        ssc.sparkContext.setLogLevel("WARN")

        val Array(broker, topics) = Array("CentOS:9092", "kafkastreaming")
        val Array(hdfsUrl, fileNamePrefix, tempPath, targetPath) = Array("hdfs://CentOS:8020", "DataToHdfsStreamingAPP-", "/user/hadoop/output/temp/", "/user/hadoop/output/normal/")

        var kafkaParams = Map[String, String]()
        kafkaParams += ("bootstrap.servers" -> broker)
        val topicSet = topics.split(",").toSet

        val kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

        val wordCntDStream = kafkaDStream.map(_._2).flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _)

        wordCntDStream.repartition(1).foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
            println(s"======================${time}======================")
            println("总数 : " + rdd.count())
            rdd.filter()
            saveRddAsTextFileAndMerge[String, Int](hdfsUrl, fileNamePrefix, time.milliseconds, tempPath, targetPath, rdd)
        })

        ssc.start()
        ssc.awaitTermination()
    }

    def saveRddAsTextFileAndMerge[K: ClassTag, V: ClassTag](hdfsUrl: String, fileNamePrefix: String, time: Long, tempPath: String, targetPath: String, rdd: RDD[(K, V)]): Unit = {
        // 先保存到 temp 路径下
        val tempFullPath = hdfsUrl + tempPath
        // GzipCodec 不可分割，  BZip2Codec 可分割
        rdd.saveAsTextFile(tempFullPath + "/" + time, classOf[BZip2Codec])
        // 只有 PairRDD 才能使用下面这种hadoop api来保存rdd， 如果是普通RDD，只能使用上面那种方法
        // rdd.saveAsNewAPIHadoopFile()

        if ((time / 10000) % 10 == 5) {
            // 在 merge 到目标路径
            val targetFullPath = hdfsUrl + targetPath
            val hadoopConf = rdd.sparkContext.hadoopConfiguration
            val hdfs = FileSystem.get(hadoopConf)
            //  FileUtil.copyMerge(hdfs, new Path(tempFullPath), hdfs, new Path(targetFullPath + "/" + time + ".log"), false, hadoopConf, "======================")
            mergeFile(hdfs, new Path(tempFullPath), new Path(targetFullPath + "/" + time + ".log"), hadoopConf, true, true)

        }
    }


    def mergeFile(fileSystem: FileSystem, tempPath: Path, targetPath: Path, conf: Configuration, overwrite: Boolean, deleteSource: Boolean): Unit = {
        val allFiles = listAllFile(fileSystem, tempPath)
        val out = fileSystem.create(targetPath, overwrite)
        for (file <- allFiles) {
            println(file)
            val in = fileSystem.open(file)
            IOUtils.copyBytes(in, out, conf, false)
        }
        out.close()
        listFiles.clear()
        if (deleteSource) {
            fileSystem.delete(tempPath, true)
        }
    }

    def listAllFile(fileSystem: FileSystem, path: Path): Array[Path] = {
        val files = fileSystem.listStatus(path, new PathFilter {
            override def accept(path: Path): Boolean = {
                val filter: Boolean = path.getName.endsWith("SUCCESS")
                !filter
            }
        })

        for (f <- files) {
            if (f.isDirectory) {
                listAllFile(fileSystem, f.getPath)
            } else if (f.isFile) {
                listFiles += f.getPath
            }
        }
        listFiles.toArray
    }
}