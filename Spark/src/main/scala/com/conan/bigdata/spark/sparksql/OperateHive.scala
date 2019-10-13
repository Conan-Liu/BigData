package com.conan.bigdata.spark.sparksql

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

case class Record(k: Int, v: String)

object OperateHive {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName(OperateHive.getClass.getName)
            // 从 2.0 开始不再推荐使用hive-site.xml文件中的 hive.metastore.warehouse.dir
            // spark.sql.warehouse.dir 来代替
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .enableHiveSupport()
            .getOrCreate()

        // 引入 隐式转换
        import spark.implicits._

        // 查询
        val sqlDF = spark.sql("select cityid as city_id,name as city_name,provinceid as province_id,province as province_name,'1' as flag from ods.resta_citytable")
        val sqlDF1 = spark.sql("select cityid as city_id,name as city_name,provinceid as province_id,province as province_name,provinceid%2 as p1,cityid%3 as p2 from ods.resta_citytable")
        // 这里的case Row 是模式匹配,字段名称可以不一样， 但是建议写成一致的， 字段个数一定要一致， 否则报错
        val caseDF = sqlDF.map {
            case Row(a: Int, city_name: String, province_id: Int, province_name: String, flag: String) => {
                s"$a, $city_name, $province_id, $province_name"
            }
        }.show(10, false)
        sqlDF.show()

        // 如果case class 定义在这里， 虽然可能返回DF， 但是这个DF实例不能调用createOrReplaceTempView
        val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
        recordsDF.createOrReplaceTempView("record")

        // 创建表, 正常情况下，都是在hive侧创建好了表，直接往里面写数据或读数据
        spark.sql("CREATE TABLE hive_records(key int, value string) STORED AS PARQUET")
        // 读取表
        val tableDF = spark.table("ods.resta_citytable")
        tableDF.printSchema()
        tableDF.select("cityid","name").show()
        // 写表
        // partitionBy 可以用来指定分区， 以parquet（spark默认格式）的格式存储数据
        // 可以hive 先创建分区表，spark负责插入数据
        // 语句执行完， 在hive客户端里面可以看到这个分区已经有了，flag=1 的分区，是可以直接查询的, 说明是直接操作 hive metastore 的
        sqlDF.write.partitionBy("flag").format("parquet").mode(SaveMode.Overwrite).saveAsTable("temp.city")
        // 多个动态分区
        sqlDF1.write.partitionBy("p1", "p2").format("parquet").mode(SaveMode.Overwrite).saveAsTable("temp.city_1")


        // 生成parquet文件，并创建hive表，可供查询
        val dataDir = ""
        spark.range(10).write.parquet(dataDir)
        spark.sql("create table .... location '...'")


        // 配置hive的参数, 推荐直接使用sparksession的变量来访问conf， 不要用sqlcontext
        spark.conf.get("hive.exec.dynamic.partition.mode")  // 推荐
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        spark.sqlContext.getConf("hive.exec.dynamic.partition.mode")  // 不推荐
        spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        tableDF.write.partitionBy("key").format("hive").saveAsTable("")
    }

}