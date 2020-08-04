package com.conan.bigdata.common.api

import java.net.{HttpURLConnection, URL}
import java.util

object Test {

    def doPost(url: String, content: String) {
        val restURL = new URL(url)
        val httpConn = restURL.openConnection.asInstanceOf[HttpURLConnection]
        httpConn.setRequestMethod("POST")
        httpConn.setRequestProperty("Content-Type", "application/json")
        httpConn.setDoOutput(true)
        httpConn.setAllowUserInteraction(false)
        val out = httpConn.getOutputStream
        out.write(content.getBytes)
        out.flush()
        val in = httpConn.getInputStream
        out.close()
        in.close()
    }

    def main(args: Array[String]): Unit = {
        val list=new util.ArrayList[String]()
        if(list==null||list.size()==0){
            println("true")
        }
    }
}
