package com.conan.bigdata.flink.scalaapi.source

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
  * 自定义数据源实现生成订单数据
  */
case class Order(orderId:String,userId:Int,money:Long,createTime:Long)
class OrderCustomSourceFunction extends RichParallelSourceFunction[Order]{
var isRunning=true

    override def run(ctx: SourceContext[Order]):Unit = {
        while (isRunning){
            val orderId: String = UUID.randomUUID().toString
            val userId:Int=Random.nextInt(3)
            val money:Long=Random.nextInt(101)
            val createTime:Long=System.currentTimeMillis()
            ctx.collect(Order(orderId,userId,money,createTime))
            TimeUnit.SECONDS.sleep(1)
        }
    }

    override def cancel():Unit = {
        isRunning=false
    }
}


object OrderCustomSourceFunction{
    def main(args: Array[String]): Unit = {
        val senv=StreamExecutionEnvironment.getExecutionEnvironment

        val source = senv.addSource(new OrderCustomSourceFunction()).setParallelism(1)
        source.print()
        senv.execute("aaa")
    }
}