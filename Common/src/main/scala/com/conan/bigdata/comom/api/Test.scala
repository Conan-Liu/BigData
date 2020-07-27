package com.conan.bigdata.comom.api

import java.net.{HttpURLConnection, URL}

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
        val wxBody = "{\"type\":\"2\",\"receiverMobiles\":\"13852293070\",\"subject\":\"测试 subject\",\"content\":\"aa\"}\"}"
        doPost("http://alarm-notify.mwbyd.cn/services/notify/pushAll", wxBody)
    }
}
