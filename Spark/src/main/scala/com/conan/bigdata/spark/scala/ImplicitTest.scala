package com.conan.bigdata.spark.scala

/**
  */
object ImplicitTest {

    // 隐式转换：  默认编译器给你执行的函数， 由implicit修饰， 带一个参数的特殊函数
    // 下面这个就是一个隐式函数， 传入string类型的数据，可以默认自动给你转成int型
    implicit def str2Int(s: String): Int = Integer.parseInt(s)

    // 定义一个隐式参数， 可以不用指定， 从上下文中推断出来
    // 隐式参数 ， 只能用于柯里化函数
    def implicitParams(a: Int)(implicit b: Double, c: Double): Double = {
        a * b / c
    }
//   一个参数列表也是可以的
//    def implicitParams(implicit a: Int): Double = {
//        a * 2
//    }

    def showInt(a: Int) = println(a)

    //    def main(args: Array[String]): Unit = {
    //        // 这里定义的shouInt方法是需要int型的参数， 但是传入的却是字符串的， 如果没有上面那个 implicit 方法，
    //        // 那么这里就会报错， 可以注释看看效果， 这里默认给你调用隐式函数， 完成类型转换
    //        showInt("12")
    //    }

}

object TestImportImplicit {

    def showInt1(a: Int) = println(a)

    def main(args: Array[String]): Unit = {
        // 因为隐式函数不在一个object下面，不能访问到， 所以报错， 需要使用import 语法导入才行
        // import 单例对象._   下划线表示该单例对象下所有的函数
        import ImplicitTest._
        showInt1("12")

        // 下面就是隐式参数， 必须要 implicit 修饰才行， 直接传参数也行
        // 当有多个隐式参数的时候， 就不好判断哪个值传给哪个参数了
        implicit val d = 1
        implicit val e = 2.3
        println(implicitParams(2))
        println(implicitParams(2)(2.5, 5))
    }
}