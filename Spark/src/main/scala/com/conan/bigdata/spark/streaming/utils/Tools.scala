package com.conan.bigdata.spark.streaming.utils

import org.apache.spark.SparkContext
import org.apache.spark.streaming.Time
import org.apache.spark.util.LongAccumulator

object Tools {

    @volatile private var acc: LongAccumulator = _

    def getAccInstance(sc: SparkContext, time: Time): LongAccumulator = {
        if (acc == null) {
            // TODO ... 这个怎么用啊
            synchronized {
                if (acc == null) {
                    acc = sc.longAccumulator("timer")
                    acc.add(time.milliseconds)
                }
            }
        }
        acc
    }
}
