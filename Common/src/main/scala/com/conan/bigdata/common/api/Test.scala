package com.conan.bigdata.common.api

import java.util.Properties

object Test {

    lazy val properties:Properties=init

    def init(): Properties ={
        println("=================")
        val p=new Properties()
        p.put("a","aa")
        p
    }

    def main(args: Array[String]): Unit = {
        println("1")
        println(properties.getProperty("a"))
        println("2")
        println(properties.getProperty("a"))
    }
}
