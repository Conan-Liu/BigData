package com.conan.bigdata.spark.streaming.simple.bean


/**
  * 课程用户点击数
  * @param dayCourseid  对应天的课程
  * @param clickCount      PV数
  */
case class CourseClickCount(dayCourseid:String,clickCount:Long)