package com

import java.util.Properties

import com.conan.bigdata.kafka.util.Constants
import kafka.admin.AdminClient
import  scala.collection.JavaConverters._

object Test {

    def main(args: Array[String]): Unit = {
        val properties = new Properties
        properties.put("bootstrap.servers", Constants.BROKER_LIST)
        val adminClient = AdminClient.create(properties)

        val coordinator = adminClient.findCoordinator(Constants.GROUP_ID_1, 1000)
        val groupSummary = adminClient.describeConsumerGroupHandler(coordinator, Constants.GROUP_ID_1)
        val members = groupSummary.members()
        members.asScala.foreach(member=>{
            val memberId=member.memberId()
            val clientId=member.clientId()
            val host = member.clientHost()
            println("========")
            println(s"${host} => ${memberId}, ${clientId}")
        })
    }
}
