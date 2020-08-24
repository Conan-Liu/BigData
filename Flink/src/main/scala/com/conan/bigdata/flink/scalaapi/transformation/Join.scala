package com.conan.bigdata.flink.scalaapi.transformation

import java.io.Serializable

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala._

/**
  * 演示join的相关操作
  */
object Join {

    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val score = env.fromCollection(Array[(Int, Int, Double)]((1, 10, 43.5), (2, 20, 90), (2, 30, 90)))
        val subject = env.fromCollection(Array[(Int, String)]((10, "语文"), (20, "数学"), (40, "英语")))

        // 相当于sql的score a inner join subject b on a.1 = b.0，字段下表从0开始
        // 可以指定多个关联字段where(0,1).equalTo(1,2)，关联的字段要按顺序排好
        val joinDS: JoinDataSet[(Int, Int, Double), (Int, String)] = score.join(subject).where(1).equalTo(0)
        // 可以使用apply方法，指定需要返回的字段，相当于select
        val apply = joinDS.apply((left,right)=>(left,right._2))
        apply.print()

        // join可以不用apply，leftOuterJoin必须使用apply来处理未关联上的数据，该方法还提供一个join优化提示，默认交给flink优化
        val leftJoinDs = score.leftOuterJoin(subject, JoinHint.OPTIMIZER_CHOOSES).where(1).equalTo(0).apply((left, right) => {
            if (right == null)
                (left, "null")
            else
                (left, right._2)
        })


        // fullOuterJoin也需要apply
        val joinAssigner: JoinFunctionAssigner[(Int, Int, Double), (Int, String)] = score.fullOuterJoin(subject).where(1).equalTo(0)
        val fullJoinDS: DataSet[(Serializable, Serializable)] = joinAssigner.apply((left, right) => {
            if (left == null) {
                ("null", right)
            } else if (right == null) {
                (left, "null")
            } else {
                (left, right)
            }
        })

        joinDS.print()
        leftJoinDs.print()
        fullJoinDS.print()
    }
}
