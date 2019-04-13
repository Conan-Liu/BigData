package com.conan.bigdata.common.leetcode;

/**
 * Created by Administrator on 2019/4/13.
 * 整数反转
 *
 * int型的数值范围为 [−2^31,  2^31 − 1]
 * 如果把最小的整数求绝对值， 则会溢出， 最小的数求绝对值还是最小的数， 两者一样
 * 最大的数 + 1 = 最小的数
 * 最小的数 - 1 = 最大的数
 */
public class Solution7 {

    public static int reverse(int x) {
        StringBuilder sb = new StringBuilder();
        if (x >= 0) {
            sb.append(x);
            sb.reverse();
        } else {
            String x1=String.valueOf(x);
            sb.append(x1.substring(1)).append("-");
            sb.reverse();
        }
        long y = Long.parseLong(sb.toString());
        if (y > Integer.MAX_VALUE || y < Integer.MIN_VALUE) {
            return 0;
        }
        return (int) y;
    }

    public static void main(String[] args) {
        System.out.println("Int最大值:" + Integer.MAX_VALUE);
        System.out.println("Int最小值:" + Integer.MIN_VALUE);
        System.out.println(reverse(Integer.MIN_VALUE));
    }
}