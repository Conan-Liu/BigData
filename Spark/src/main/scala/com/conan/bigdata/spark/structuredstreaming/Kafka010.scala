package com.conan.bigdata.spark.structuredstreaming

import org.apache.spark.sql.streaming.OutputMode

/**
  * http://spark.apache.org/docs/2.2.3/structured-streaming-kafka-integration.html
  */
object Kafka010 extends StructuredVariable{

    def main(args: Array[String]): Unit = {

        val df=spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","structured").load()

        import spark.implicits._
        val dataset = df.selectExpr("cast(key as string)","cast(value as string)").as[(String,String)]

        val query=dataset.writeStream.outputMode(OutputMode.Append()).format("console").start()

        query.awaitTermination()
    }
}
