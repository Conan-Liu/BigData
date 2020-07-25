package com.conan.bigdata.comom.api

object Test {

    def main(args: Array[String]): Unit = {

        val t=new Thread(new Runnable {
            override def run():Unit = {
                Thread.sleep(10000)
                throw new Exception("timeout ...")
            }
        },"t")
        t.start()
        Thread.sleep(10000000)
    }
}
