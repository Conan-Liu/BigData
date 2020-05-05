package com.conan.bigdata.hadoop.basic;

import com.conan.bigdata.hadoop.mr.SelfDataTypeAndSecondSort;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义Key实现二次排序
 * hadoop 的数据类型都需要实现Writable接口来序列化
 * 特别要注意的是，K 是特殊的数据类型，在shuffle阶段需要参与排序，所以 K 的数据类型需要实现Comparable接口
 * hadoop提供排序和序列化整合的接口WritableComparable
 *
 * MapReduce参考 {@link SelfDataTypeAndSecondSort}
 */
public class WritableComparableExp implements WritableComparable<WritableComparableExp> {

    private int first;
    private int second;

    public WritableComparableExp() {

    }

    public WritableComparableExp(int first, int second) {
        set(first, second);
    }

    public void set(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public int getFirst() {
        return this.first;
    }

    public int getSecond() {
        return this.second;
    }

    // 对象比较大小， 排序
    @Override
    public int compareTo(WritableComparableExp o) {
        if (this.first != o.first) {
            // 表示 K 升序
            return this.first > o.first ? 1 : -1;
        } else if (this.second != o.second) {
            return this.second > o.second ? 1 : -1;
        } else {
            // 表示这个 K 相等
            return 0;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.first);
        out.writeInt(this.second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.first = in.readInt();
        this.second = in.readInt();
    }

    @Override
    public int hashCode() {
        // hashCode 用来确定这个 K 的所对应的 reducer,  默认是 hashCode % reducer个数
        return (first + second) * 123;
    }

    // 比较两个对象是否相等
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        } else if (this == obj) {
            return true;
        } else if (obj instanceof WritableComparableExp) {
            WritableComparableExp pair = (WritableComparableExp) obj;
            return this.first == pair.first && this.second == pair.second;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "K[cityid=" + this.first + ",id=" + this.second + "]";
    }
}