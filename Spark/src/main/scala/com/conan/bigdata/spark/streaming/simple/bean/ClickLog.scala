package com.conan.bigdata.spark.streaming.simple.bean

/**
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
