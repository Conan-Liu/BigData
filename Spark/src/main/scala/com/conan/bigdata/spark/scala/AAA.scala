package com.conan.bigdata.spark.scala

import scala.util.control.Breaks

/**
  * Created by Administrator on 2019/1/31.
  */
object AAA {

    def apply(f: (Int) => String, v: Int): String = {
        return f(v+12)
    }

    def f(v: Int): String = {
        return "[" + v + "]"
    }

    def fun(name:String):Unit={
        println(name)
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

        val loop = new Breaks
        loop.breakable(
            for (i <- 1 to 10) {
                println(i)
                if (i == 4)
                    loop.break()
            }
        )

        println(apply(f,10))

        val fun_v=fun _
        fun_v("aaaa")
    }
}