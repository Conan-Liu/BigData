package com.conan.bigdata.spark.streaming.simple.bean

/**
  * Created by Conan on 2019/5/2.
  */
/**
  * 类似于java中的 javaBean
  * @param ip
  * @param time
  * @param courseId
  * @param statusCode
  * @param referer
  */
case class ClickLog(ip:String,time:String,courseId:Int,statusCode:Int,referer:String)
