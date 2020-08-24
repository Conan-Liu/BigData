package com.conan.bigdata.flink.scalaapi.source

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.concurrent.TimeUnit

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

/**
  * 使用[[RichParallelSourceFunction]]来读取MySql的数据Source
  * 注意生产不要这么用，select查询可能会很慢，影响业务，推荐读取binlog方式来实时加载数据
  */
case class MysqlStudet(id: Int, name: String, age: Int)

class MySqlSourceFunction extends RichParallelSourceFunction[MysqlStudet] {

    private var isRunning = true
    private var ele: Long = 0
    private var conn: Connection = _
    private var ps: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
        // 执行一次，可以用来打开数据源
        conn = DriverManager.getConnection("", "", "")
        ps = conn.prepareStatement("select id,name,age from t_student")
    }

    override def run(ctx: SourceContext[MysqlStudet]): Unit = {
        while (isRunning) {
            val result: ResultSet = ps.executeQuery()
            while (result.next()) {
                val id = result.getInt("id")
                val name = result.getString("name")
                val age = result.getInt("age")
                ctx.collect(MysqlStudet(id, name, age))
            }
            TimeUnit.SECONDS.sleep(4)
        }
    }

    override def cancel(): Unit = {
        // 取消发送数据
        isRunning = false
    }

    override def close(): Unit = {
        // 执行一次，可以关闭数据源
        if (ps != null)
            ps.close()
        if (conn != null)
            conn.close()
    }
}

object MySqlSourceFunction {
    def main(args: Array[String]): Unit = {
        val senv = StreamExecutionEnvironment.getExecutionEnvironment
        val source = senv.addSource(new MySqlSourceFunction).setParallelism(2) // 并行数据源可以设置并行度
        source.print()

        senv.execute()
    }
}