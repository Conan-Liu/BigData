package com.conan.bigdata.spark.streaming.simple.bean

/**
  * 从搜索引擎过来的课程
  * @param daySearchCourse  日搜索课程
  * @param clickCount  pv
  */
case class CourseSearchClickCount(daySearchCourse:String,clickCount:Long)