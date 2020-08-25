package com.conan.bigdata.flink.scalaapi.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.scala._

/**
  * Mysql 的Sink方法
  */
case class Student(id: Int, name: String, age: Int)

object MySqlSinkFunction {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val collection: DataStream[Student] = env.fromCollection(Array[Student](Student(1, "zhansan", 10)))
        collection.addSink(new MySqlSinkFunction)

        env.execute("aaa")
    }
}


class MySqlSinkFunction extends RichSinkFunction[Student] {

    private var ele: Long = 0
    private var conn: Connection = _
    private var ps: PreparedStatement = _

    // 执行一次，可以用来打开数据源
    override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("", "", "")
        ps = conn.prepareStatement("insert into .....")
    }

    // 执行一次，可以关闭数据源
    override def close(): Unit = {
        if (ps != null)
            ps.close()
        if (conn != null)
            conn.close()
    }

    // 负责写数据到mysql中，value就是上游传入需要写入数据表的数据
    override def invoke(value: Student, context: Context[_]): Unit = {
        ps.setString(1, value.name)
        ps.setInt(2, value.age)
        ps.executeUpdate()
    }
}