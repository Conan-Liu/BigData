package com.conan.bigdata.spark.ml

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.ml.linalg.{DenseMatrix, Matrices, Vectors=>NewVectors}
import org.apache.spark.mllib.linalg.{Vectors=>OldVectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix


/**
  * spark机器学习算法的底层实现，采用的Breeze来处理矩阵，向量和数值计算
  * spark内部自己封装了Breeze方法，可以使用spark内置的矩阵向量来完成任务
  */
object MatrixVectorExp extends SparkVariable{

    def main(args: Array[String]): Unit = {

        // 全0矩阵
        val m1 = DenseMatrix.zeros(2, 3)
        println(m1)

        // 全1矩阵
        val m2 = DenseMatrix.ones(3, 2)
        println(m2)
        // 转置矩阵
        println(m2.transpose)

        // 稠密矩阵，注意这里的数组的length应该等于numRows * numCols
        val m3 = Matrices.dense(2,4,Array[Double](1,2,3,4,5,6,0,0))
        println(m3)
        // 转为稀疏矩阵，值为0的元素不再保存，由数据可知，存储格式为 (行下标，列下标) 值
        println(m3.toSparse)

        // 不再使用DenseVector来创建向量，使用Vectors类的dense方法
        // val v1=DenseVector
        val v1 = NewVectors.zeros(10)
        println(v1)
        val v2 = NewVectors.dense(Array[Double](0, 3, 0, 0,2))
        println(v2)
        // 转为稀疏向量，值为0的元素不再保存，由数据可知，存储格式为 [下标] [值]
        println(v2.toSparse)

        distributedMatrix()
    }

    /**
      * Matrix是单机矩阵，Spark实现了分布式矩阵
      */
    def distributedMatrix(): Unit ={
        val data=sc.parallelize(Seq[Array[Double]](
            Array(1,2.0,2.1,0.4),
            Array(2,5.3,2.6,0.8),
            Array(3,4.6,5.5,1.2)
        )).map(x=>OldVectors.dense(x))

        val matrix=new RowMatrix(data)
        println(matrix)
    }
}