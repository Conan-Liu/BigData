package com.conan.bigdata.common.api

import java.net.{HttpURLConnection, URL}

class ScalaAPI {

    // jdk 的Post请求
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
        val resultCode = httpConn.getResponseCode
        if (HttpURLConnection.HTTP_OK == resultCode) {
            val b = new Array[Byte](4096)
            var len = in.read(b)
            val sb = new StringBuilder

            // 注意这里while条件的写法，和java的写法有点不同，while ((len = in.read(b)) != -1)，java的写法在这里会报错StringIndexOutOfBoundsException，谨记scala语法
            // 其它的操作流估计都有这种问题
            while (len > -1) {
                sb.append(new String(b, 0, len, "UTF-8"))
                len = in.read(b)
            }
            println(sb.toString())
        }
        out.close()
        in.close()
    }

}
