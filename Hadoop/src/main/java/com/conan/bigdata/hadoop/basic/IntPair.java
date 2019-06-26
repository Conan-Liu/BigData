package com.conan.bigdata.hadoop.basic;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2019/6/26.
 * <p>
 * 自定义Key实现二次排序
 */
public class IntPair implements WritableComparable<IntPair> {

    private int first;
    private int second;

    public IntPair() {

    }

    public IntPair(int first, int second) {
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
    public int compareTo(IntPair o) {
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
        } else if (obj instanceof IntPair) {
            IntPair pair = (IntPair) obj;
            return this.first == pair.first && this.second == pair.second;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "[" + this.first + "\t" + this.second + "]";
    }
}