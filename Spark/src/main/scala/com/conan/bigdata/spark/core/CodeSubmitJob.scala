package com.conan.bigdata.spark.core

import java.util.concurrent.CountDownLatch

import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.launcher.SparkAppHandle.Listener
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

/**
  * 代码提交Spark任务，实现spark-submit的功能
  *
  * 官网推荐使用[[org.apache.spark.launcher.SparkLauncher]]，该类还是调用spark-submit脚本
  * 所以和使用spark-submit传入一样的参数，线上环境需要添加SparkLauncher的依赖jar包
  *
  * spark-submit使用类[[org.apache.spark.deploy.SparkSubmit]]来提交任务
  *
  * SparkSubmit类使用
  * [[org.apache.spark.deploy.yarn.Client]]提交任务到Yarn上
  * [[org.apache.spark.deploy.Client]]提交任务到Standalone上
  *
  * 还有一种方式,利用提供的 restful api 来提交,可以参考网络
  */
object CodeSubmitJob {

    // TODO... 尚未运行成功
    def main(args: Array[String]): Unit = {

        val launcher = new SparkLauncher()
        launcher.setSparkHome("E:\\Apache\\Spark\\spark-2.2.3-bin-hadoop2.6")
            .setAppName("hahaha").setMaster("local[*]")
            .setAppResource("E:\\Project\\BigData\\BigData\\Spark\\target\\spark-1.0-SNAPSHOT.jar")
            .setMainClass("com.conan.bigdata.spark.ml.ItemCF")
            .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
            .addAppArgs("来自于SparkLauncher")

        launch1(launcher)
    }

    def launch1(launcher: SparkLauncher): Unit = {
//        val countDownLatch = new CountDownLatch(1)
        val handler = launcher.startApplication(new Listener {
            override def infoChanged(handle: SparkAppHandle): Unit = {
                println("**********infoChanged*************")
            }

            override def stateChanged(handle: SparkAppHandle): Unit = {
                println("**********stateChanged*************")
                // 这里监听任务状态，当任务结束时（不管是什么原因结束）,isFinal（）方法会返回true,否则返回false
                if (handle.getState.isFinal){

                    //                    countDownLatch.countDown()
                }
            }
        })

        println("The task is executing, please wait ....")
        Thread.sleep(20000)
//        countDownLatch.await()
        println(s"id : ${handler.getAppId}")
        println(s"state : ${handler.getState}")

        // while (SparkAppHandle.State.FINISHED != handler.getState) {
        // 可以睡眠来阻塞，等待Spark任务结束
        //  Thread.sleep(10000)
        //}
    }

    /**
      * 通过SparkLanuncher.lanunch()方法获取一个进程
      * 然后调用进程的process.waitFor()方法等待线程返回结果
      * 但是使用这种方式需要自己管理运行过程中的输出信息
      */
    def launch2(launcher: SparkLauncher): Unit = {
        val processer = launcher.launch()
        // 自己写代码把三个日志流输出到指定地方，可以是文件
        val in = processer.getInputStream
        val out = processer.getOutputStream
        val err = processer.getErrorStream
        processer.waitFor()
    }

    // 提交到本地，该代码不可执行
    def localSubmit(): Unit = {
        val params: Array[String] = Array(
            "--name", "hahaha",
            "--master", "local[*]",
            "--class", "com.conan.bigdata.spark.ml.ItemCF",
            "E:\\Project\\BigData\\BigData\\Spark\\target\\spark-1.0-SNAPSHOT.jar"
        )
        SparkSubmit.main(params)
    }
}