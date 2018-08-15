package com.conan.bigdata.spark

import org.apache.log4j.{Level, Logger}

/**
  * Hello world!
  *
  */
object App {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
    }
}
