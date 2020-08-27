package com.conan.bigdata.common.api

object Test {

    case class Stu(sno:Int,name:String,sex:Int,createTime:Long){
        override def toString: String = {
            s"${sno}|${name}|${sex}|${createTime}"
        }
    }
    def main(args: Array[String]): Unit = {
        println(Stu(1,"hahah",1,1234567890))
    }
}
