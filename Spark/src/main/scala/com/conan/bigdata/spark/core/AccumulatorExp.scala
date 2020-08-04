package com.conan.bigdata.spark.core

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.TaskContext

object AccumulatorExp extends SparkVariable{

    def main(args: Array[String]): Unit = {

        // 这里的sum是定义在Driver的变量
        var sum=0
        val sumAcc=sc.longAccumulator("sum")

        val rdd=sc.parallelize[Int](Array(1,2,3,4,5,6),2)

        println(s"总分区数:${rdd.getNumPartitions}")
        rdd.map(x=>{
            // sum在executor执行计算，打印结果看，执行了加法运算
            sum=sum+x
            sumAcc.add(x)
            println(s"分区id: ${TaskContext.getPartitionId()}， sum=${sum}， sumAccCnt=${sumAcc.count}， sumAccSum=${sumAcc.sum}")
        }).collect()

        sumAcc.add(100)

        // 这里虽然executor对sum进行了加法的修改，但是没有返回给Driver端，所以打印 0
        // 由此可得，Driver端定义的共享变量会拷贝一份到executor端执行计算，但是并不会返回
        // 累加器可以返回Driver
        println(s"sum=${sum}， sumAccCnt=${sumAcc.count}， sumAccSum=${sumAcc.sum}")

        sc.stop()
    }
}
