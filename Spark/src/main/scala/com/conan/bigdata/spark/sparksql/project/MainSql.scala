package com.conan.bigdata.spark.sparksql.project

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.cli.{BasicParser, Options}
import org.slf4j.LoggerFactory

/**
  * Spark Sql 项目的入口类
  */
object MainSql {

    private val log = LoggerFactory.getLogger("MainSql")
    private val sdf = new SimpleDateFormat("yyyy-MM-dd")

    def main(args: Array[String]): Unit = {
        val options = new Options()
        options.addOption("c", true, "processr class name")
        options.addOption("d", true, "target date")
        try {
            val parser = new BasicParser
            val cmd = parser.parse(options, args)
            val className = cmd.getOptionValue("c", "MainSql")
            val targetDate = cmd.getOptionValue("d", sdf.format(new Date))
            log.info(s"className:[${className}], targetDate:[${targetDate}]")
            execute(className, targetDate)
        } catch {
            case _: Exception => println("exception...")
        }
    }


    def execute(className: String, targetDate: String): Unit = {
        val clz = Class.forName(className)
        val shortClassName = clz.getSimpleName
        // 反射获取对象
        val instance = clz.newInstance().asInstanceOf[Processor]
        instance.reset(targetDate)
        instance.execute(targetDate)
        instance.close()
    }
}
