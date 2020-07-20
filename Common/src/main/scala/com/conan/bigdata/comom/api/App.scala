package com.conan.bigdata.comom.api

import java.util.Properties

object App {

    lazy val connection: String = init

    def init: String = {
        println("==========")
        "lazy string"
    }

    lazy val properties: Properties = get

    def get: Properties = {
        println("=======")
        val p = new Properties()
        p.setProperty("aaaa", "bbbb")
        p
    }

    var aaa: String = _

    def main(args: Array[String]): Unit = {
        val t1=new Thread(new Runnable {
            override def run():Unit = println(properties.getProperty("aaaa"))
        })

        val t2=new Thread(new Runnable {
            override def run():Unit = println(properties.getProperty("aaaa"))
        })

        t1.start()
        t2.start()
    }
}
