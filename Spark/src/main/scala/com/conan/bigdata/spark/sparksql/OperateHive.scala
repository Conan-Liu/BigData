package com.conan.bigdata.spark.sparksql

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

case class Record(k: Int, v: String)

object OperateHive {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName(OperateHive.getClass.getName)
            // 从 2.0 开始不再推荐使用hive-site.xml文件中的 hive.metastore.warehouse.dir
            // spark.sql.warehouse.dir 来代替
            .config("spark.sql.warehouse.dir", "")
            .enableHiveSupport()
            .getOrCreate()

        // 引入 隐式转换
        import spark.implicits._

        // 查询
        val sqlDF = spark.sql("select f1,f2 from tab1")
        sqlDF.map {
            case Row(f1: Int, f2: String) => {
                println(s"f1 : $f1, f2 : $f2")
                f1.toString + f2
            }
        }
        sqlDF.show()

        // 如果case class 定义在这里， 虽然可能返回DF， 但是这个DF实例不能调用createOrReplaceTempView
        val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
        recordsDF.createOrReplaceTempView("record")

        // 创建表
        spark.sql("CREATE TABLE hive_records(key int, value string) STORED AS PARQUET")
        // 读取表
        val tableDF = spark.table("")
        // 写表
        tableDF.write.mode(SaveMode.Overwrite).saveAsTable("")


        // 生成parquet文件，并创建hive表，可供查询
        val dataDir = ""
        spark.range(10).write.parquet(dataDir)
        spark.sql("create table .... location '...'")


        // 配置hive的参数
        spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        tableDF.write.partitionBy("key").format("hive").saveAsTable("")
    }

}