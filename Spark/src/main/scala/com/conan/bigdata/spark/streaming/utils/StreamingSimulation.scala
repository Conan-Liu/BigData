package com.conan.bigdata.spark.streaming.utils

import java.io.PrintWriter
import java.net.ServerSocket
import java.util.Random

import scala.io.Source

/**
  * Created by Administrator on 2017/1/5.
  */
object StreamingSimulation {
  def index(length: Int): Int = {
    val rand = new Random()
    return rand.nextInt(length)
  }

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: <filename> <port> <millisecond>")
      System.exit(1)
    }

    val filename = args(0)
    val lines = Source.fromFile(filename).getLines.toList
    val filerow = lines.length

    val listener = new ServerSocket(args(1).toInt)
    while (true) {
      val socket = listener.accept();
      new Thread() {
        override def run = {
          println("Get client connected from : " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream, true)
          while (true) {
            Thread.sleep(args(2).toLong)
            val content = lines(index(filerow))
            println(content)
            out.write(content + '\n')
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
