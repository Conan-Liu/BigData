package com.conan.bigdata.flink.scalaapi.source

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  * DataSource主要两类
  * 1. 基于集合
  * 2. 基于文件
  * 该类时基于文件的演示
  * 可以读取gzip，bzip2压缩的文件，不需要指定压缩类，不能并行读取，spark也可以直接读取这两种格式的，不需要修改代码
  * 默认不循环递归读取文件目录下的子目录，可以通过修改参数"recursive.file.enumeration"来递归读取子目录
  */
object FileExp {

    case class Student(sno: Int, name: String)

    def main(args: Array[String]): Unit = {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val conf: Configuration = new Configuration()
        conf.setBoolean("recursive.file.enumeration", true)

        // 读取本地文件
        val file: DataSet[String] = env.readTextFile("/Users/mw/temp/w")
        // 递归读取文件目录
        val file1: DataSet[String] = env.readTextFile("/Users/mw/temp/file").withParameters(conf)

        // 读取csv文件，需要泛型，还要注意如果格式不正确则报错
        // case class Student(sno: Int, name: String) // 不能写在这，否则报错，需要写在方法外面，这是为什么？
        val csvDS: DataSet[Student] = env.readCsvFile[Student]("/Users/mw/temp/student")

        // 读取压缩文件，如果是gzip，bzip2压缩，可以直接读取
        val fileCompress: DataSet[String] = env.readTextFile("/Users/mw/temp/file/w.gz")

        file.print()
        file1.print()
        csvDS.print()
        fileCompress.print()
    }
}
