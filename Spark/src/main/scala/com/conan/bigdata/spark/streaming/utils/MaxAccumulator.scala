package com.conan.bigdata.spark.streaming.utils

import org.apache.spark.util.AccumulatorV2

/**
  * 累计器需要满足交换律和结合律才行
  */
class MaxAccumulator extends AccumulatorV2[Long, Long] {

    private var _max = 0L
    private var _count = 0L

    override def isZero: Boolean = _max == 0L && _count == 0L

    override def copy(): AccumulatorV2[Long, Long] = {
        val newAcc = new MaxAccumulator
        newAcc._max = this._max
        newAcc._count = this._count
        newAcc
    }

    override def reset(): Unit = {
        _max = 0L
        _count = 0L
    }

    override def add(v: Long): Unit = {
        _max = if (_max < v) v else _max
        _count += 1
    }

    override def merge(other: AccumulatorV2[Long, Long]): Unit = {
        other match {
            case o: MaxAccumulator =>
                _max = if (_max < o._max) o._max else _max
                _count += o._count
            case _ =>
                throw new UnsupportedOperationException(
                    s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
        }
    }

    override def value: Long = _max
}