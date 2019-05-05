package com.conan.bigdata.spark.scala

import com.conan.bigdata.spark.streaming.WordCountToMysql

import scala.collection.immutable.HashMap
import scala.util.control.Breaks._

/**
  * Created by Administrator on 2019/1/31.
  */
object AAA {

    def apply(f: (Int) => String, v: Int): String = {
        return f(v + 12)
    }

    def f(v: Int): String = {
        return "[" + v + "]"
    }

    def fun(name: String): Unit = {
        println(name)
    }

    def myPrint = println("2222222222")

    def testOption(): Unit = {
        var map = new HashMap[String, String]()
        map += ("1" -> "liu")
        map += ("2" -> "fei")
        val s = map.get("3")
        println("Option ====" + s.getOrElse("aaa").length)
    }

    def main(args: Array[String]): Unit = {
        scala.Symbol("x")
        println("this is \" hahah")
        println(
            """shiwomen
              |mei
              |zizi
              |哈哈哈""".stripMargin)

        println("\141")

        // 如果引入  scala.util.control.Breaks._ , 这个包下的所有成员 ， 可以参考下面的代码编写方法
        //        val loop = new Breaks
        //        loop.breakable(
        //            for (i <- 1 to 10) {
        //                println(i)
        //                if (i == 4)
        //                    loop.break()
        //            }
        //        )

        println(apply(f, 10))

        val fun_v = fun _
        fun_v("aaaa")

        for (i <- 1 to 10) {
            println("下面跳出")
            //            break
            println("不执行")
        }

        // for 高级循环， 相当于嵌套两层循环，以此类推
        for (i <- 1 to 3; j <- 2 to 5 if i < j) {
            printf("i=%d, j=%d, res=%d", i, j, i * j)
            println
        }


        // 惰性变量只能是不可变变量 , 也就是 val ， 而不能是 var, 只有用到对象时才会调用实例化方法,并且无论缩少次调用，实例化方法只会执行一次。
        lazy val a: Int = 1

        val multiDim = Array.ofDim[Int](3, 4)
        println("数组长度:" + multiDim.length + "\t元素长度:" + multiDim(0).length)

        // 定义一个元组
        val t4 = Tuple4(1, "liu", "male", false)
        println(t4._3)
        val (x1, x2, x3, x4) = t4
        println(x1 + ", " + x2 + ", " + x3 + ", " + x4)


        myPrint


        println("aaaa" + "bbb")

        println("2019-03-02".endsWith("01"))

        val s: String = "a"

        // 测试关键字Option
        testOption()

        //测试 redis 链接
        WordCountToMysql.createJedisConnection()

    }

}