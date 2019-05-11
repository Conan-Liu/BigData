package com.conan.bigdata.spark.streaming.utils

import java.io.{ObjectInputStream, ObjectOutputStream}

import com.conan.bigdata.spark.rdbms.{DML, JDBCPool}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import scala.collection.JavaConverters._


/**
  * Created by Conan on 2019/5/10.
  * 这个是伴生对象， 不能指定泛型传递参数， 不够弹性
  */
object BroadcastWrapperObject{

    @volatile private var instance: Broadcast[Seq[String]] = _

    def update(sc:SparkContext, blocking: Boolean = false): Unit = {
        if (instance != null) {
            // 删除RDD前是否锁定
            instance.unpersist(blocking)
        }

        instance = sc.broadcast(DML.getAllBlackListData(JDBCPool.getInstance().borrowObject(), "black_list").asScala)
    }

    def getInstance(sc:SparkContext): Broadcast[Seq[String]] = {
        if (instance == null) {
            synchronized {
                if (instance == null) {
                    instance = sc.broadcast(DML.getAllBlackListData(JDBCPool.getInstance().borrowObject(), "black_list").asScala)
                }
            }
        }
        instance
    }

}