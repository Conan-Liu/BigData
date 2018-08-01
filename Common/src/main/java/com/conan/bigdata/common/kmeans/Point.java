package com.conan.bigdata.common.kmeans;

public class Point {

    public double[] x;
    public int len_arr;
    public boolean isSample = false;
    public int id;
    public String text;

    // 一个样本点Point可以有多维，用数组表示维度
    // 比如说（x, y），可以表示为 元素个数为 2 的数组 。 double[2] x
    public Point(double[] x, int len_arr, boolean isSample, int id) {
        this.x = x;
        this.len_arr = len_arr;
        this.isSample = isSample;
        this.id = id;
    }

    // 计算欧式距离
    public double distance(Point other) {
        double sum = 0;
        for (int i = 0; i < len_arr; i++) {
            sum += Math.pow(x[i] - other.x[i], 2);
        }
        sum = Math.sqrt(sum);
        return sum;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    // 判断样本点是否相等，这里如果样本点的id相等 就认为是同一个样本点
    public boolean equals(Object other) {
        if (other instanceof Point)
            return this.id == ((Point) other).id;
        else
            return false;
    }

    public int hashCode() {
        return this.id;
    }
}
