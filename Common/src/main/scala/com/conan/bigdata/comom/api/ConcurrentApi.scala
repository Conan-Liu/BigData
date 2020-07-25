package com.conan.bigdata.comom.api


class ConcurrentApi {

    /**
      * synchronized 方法
      * def func(): Unit = this.synchronized {
      *     code...
      * }
      */
    def func(): Unit = this.synchronized {
        println("code ...")
    }

    /**
      * scala synchronized 代码块和java的代码块类似
      * obj.synchronized {
      *     code...
      * }
      */
    def func1(): Unit = {
        this.synchronized {
            println("code ...")
        }

        val ss="abc"
        ss.synchronized{
            println("code ...")
        }
    }
}
