package com.conan.bigdata.spark.core

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  *
  */
object Test extends SparkVariable {

    def main(args: Array[String]): Unit = {
        val f = "/user/hive/warehouse/dw.db/assoc_user_tag_new_tmp1/000000_0"
        val conf = sc.hadoopConfiguration
        val exitStatus1 = FileSystem.get(conf).exists(new Path(f))
        if (exitStatus1) {
            println("true")
        } else {
            println("false")
        }

        val fs=FileSystem.get(conf)
        val exitStatus2 = fs.exists(new Path(f))
        if (exitStatus2) {
            println("true")
        } else {
            println("false")
        }
    }
}
