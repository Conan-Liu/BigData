package com.conan.bigdata.spark.scala

import com.conan.bigdata.spark.streaming.WordCountToMysql

import scala.collection.immutable.HashMap
import scala.util.control.Breaks._

/**
  */
// 伴生类
// 类后面使用小括号声明的参数列表，其实就是主构造函数的参数列表
class ScalaAPI(a: Int, b: String) {
    // 除了方法定义def， 其他的都属于主构造函数方法体，new的时候都会执行
    // 如 try-catch 会执行
    try {
        throw new Exception("Exception ...")
    } catch {
        case e: Exception => e.printStackTrace()
            println("a = " + a + ", b = " + b)
    }
    // 这也会执行
    println("主构造函数，只要new，构造函数体就会执行...")

    // 辅助构造函数，辅助构造函数一定要直接或者间接地调用主构造函数，因为主构造函数体不执行，类体就无法初始化
    // 无参辅助够造函数
    def this() {
        this(111, "Fei")
    }

    // 主构造函数调用类的成员方法，也会执行
    this.show()

    def show(): Unit = {
        println("这不会自动执行，属于成员方法，需要被调用才执行...")
    }

    // 此方法不是构造方法，是一个普通方法，只是巧了和类名一致
    def AAA(): Unit = {
        println("这不是构造方法，是一个普通方法...")
    }
}

// 伴生对象
object ScalaAPI {

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
        for (m <- map) {
            println(m._1 + "\t" + m._2)
        }

        // 使用Option类
        // unbound placeholder parameter 注意，如果这里定义 _ 作为占位符，一定要是可变类型var，否则就会报前面的错
        val isNull: String = null
        val opNull = Option[String](isNull).getOrElse("*********")
        println(s"Option例子: ${opNull}")
        // 拋异常，不会自动转变成 null，下面代码不可执行
        // println(s"测试拋异常: ${Option[Int]("abc".toInt).getOrElse(0)}")
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

        val t1 = Tuple2(1, 2)
        val (t11, t12) = t1
        println("=====" + t11 + "\t" + t12)

        // 惰性变量只能是不可变变量 , 也就是 val ， 而不能是 var, 只有用到对象时才会调用实例化方法,并且无论多少次调用，实例化方法只会执行一次。
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
        //        WordCountToMysql.createJedisConnection()

        val strs = "a,b,c,d,e".split(",")
        println(strs.take(3).mkString)
        println(strs.takeRight(3).mkString)
        println(strs.drop(2).mkString("-"))
        println(strs.dropRight(2).mkString)


        // 测试伴生类
        val aaa = new ScalaAPI(123, "Liu")
        val bbb = new ScalaAPI()

        // flatMap
        var words = Set(List('s', 'c', 'd'), List('d', 'c'))
        // flatMap 接受的是一个可迭代集合
        val wordsFlatMap = words.flatMap(x => {
            // 什么类型调用的flatMap方法，则返回的也是什么类型
            // 所以这里返回是Set集合，有去重的效果，结果为Set(c, d)
            // List(c, d)膨胀为 c和d，List(c)膨胀为c
            // 最后得到结果是Set集合，Set(c,d)
            println(x + "\t" + x.tail)
            x.tail
        })
        println(wordsFlatMap)

        // 模式匹配，如下两种写法效果一样，同一种语法的两种写法，第二种写法是匿名函数
        val arrPatternMatch = Array(1, 2, 4, 2, 5, 10)
        arrPatternMatch.map { x => x match {
            case 1 => 1*100
            case _ => println("other");0
        }
        }
        arrPatternMatch.map {
            case 1 => 1*100
            case _ => println("other");0
        }
    }
}