package com.conan.bigdata.common.api

import java.sql.Connection

import scalikejdbc._
import scalikejdbc.config.DBs

import scala.collection.mutable.ListBuffer

/**
  * scalikejdbc使用typesafe读取配置文件，默认读取application.conf  application.properties
  */
object ScalikeJdbcExp {

    /**
      * 演示批量插入
      */
    def batchInsert(list: ListBuffer[(Int, Int)]): Unit = {
        DBs.setup()
        for (i <- 1 to 10) {
            println(i)
            DB.localTx(implicit session => {
                // 这种写法相当于PrepareStatement
                val sql = SQL("insert into t1(f1, f2) values(?, ?)")
                for (x <- list) {
                    sql.bind(x._1, x._2).update().apply()
                }
            })
            Thread.sleep(10000)
        }
        // 程序运行结束需要关闭连接池
        DBs.close()
    }

    /**
      * 演示连接池的使用
      * 只有在多线程或者单线程多实例的环境下，线程池才有效，单线程执行时如果不多次new，则最多就一个连接，无需线程池
      */
    class PoolExp(conn: Connection, name: Int) extends Runnable {
        override def run(): Unit = {
            using(DB(conn)) {
                conn => {
                    // localTx()方法执行结束才算提交到mysql上insert执行成功
                    conn.localTx(implicit session => {
                        // 语句运行到apply()方法，sql语句还没有提交到mysql执行
                        SQL("insert into test(f1, f2) values(?, ?)").bind(name, "t-" + name).update().apply()
                        // 通过sleep方法，睡眠一段时间，可以查看mysql，数据并没有insert到表中
                        Thread.sleep(10000)
                    })
                }
            }
            conn.close()
        }
    }

    def connPool(list: ListBuffer[(Int, Int)]): Unit = {
        DBs.setup()
        for (i <- 1 to 10) {
            // 多线程模拟并发，每个线程都去连接池里面借连接
            val conn = ConnectionPool.borrow()
            val t = new Thread(new PoolExp(conn, i))
            t.start()
        }
        DBs.close()
    }

    def main(args: Array[String]): Unit = {
        val list = ListBuffer[(Int, Int)]()
        list.append((9, 2))
        list.append((90, 20))

//        batchInsert(list)
         connPool(list)
    }
}
