package com.conan.bigdata.spark.scala

import scala.math._

/**
  * Created by Conan on 2019/3/27.
  */
object MultiFunction {

    def add(a: Int, b: Int): Int = {
        a + b
    }

    def sub(a: Int, b: Int): Int = a - b

    def calc(a: Int, b: Int, f1: (Int, Int) => Int, f2: (Int, Int) => Int): Int = {
        if (a < b)
            f1(a, b)
        else
            f2(a, b)
    }

    def highFunction(): Unit = {
        val arr = Array(3.14, 1.42, 2.0)
        arr.map(ceil).foreach(print)

        println

        val ceilFun = ceil _ // 将函数赋值给变量 ceilFun， _ 这个符号表示确实是传递的一个函数， 而不是忘记写参数， 区别下面这行代码
        val ceilVal = ceil(2.5) // 这个是将计算的值赋给变量 ceilVal
        println(ceilVal + "=====" + ceilFun(6.5))
        arr.map(ceilFun).foreach(print)

        println
        // 下面是调用自己定义的高阶函数 calc , 定义高阶函数可以参考calc的示例， 直接使用
        // 函数名作为参数，传给高阶函数
        println("高阶函数返回值： " + calc(-8, 2, add, sub))
    }

    def anonyFunction(): Unit = {
        val arr = Array(1, 2, 3, 4)
        //        arr.map((x: Int) => x * 2;println(x*2)).foreach(print)
    }

    def main(args: Array[String]): Unit = {
        highFunction
        println
        anonyFunction
    }

}