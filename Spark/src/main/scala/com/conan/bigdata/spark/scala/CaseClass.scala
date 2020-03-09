package com.conan.bigdata.spark.scala

object CaseClass {

    abstract class Person

    case class Student(name: String, age: Int, stuNo: String) extends Person

    case class Teacher(name: String, age: Int, tecNo: String) extends Person

    case class Nobody(name: String) extends Person

    def main(args: Array[String]): Unit = {
        val p: Person = Student("lisi", 20, "101")
        p match {
            case Student(name, age, stuNo) => println(s"学生:$name, $age, $stuNo")
            case Teacher(name, age, tecNo) => println(s"老师:$name, $age, $tecNo")
            case Nobody(name) => println(s"其他人:$name")
            case _ => println("不是该类型")
        }

        val teacher = Teacher("jack", 30, "0010")
        println(teacher)
    }
}