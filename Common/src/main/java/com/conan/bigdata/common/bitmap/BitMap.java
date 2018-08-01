package com.conan.bigdata.common.bitmap;

/**
 * Created by Administrator on 2017/11/27.
 */
public class BitMap {
    public static int[] a = new int[1 + 10 / 32];

    public static void main(String[] args) {
        bitMap(10);
        bitMap(33);
        System.out.println(a[0]);
    }

    public static void bitMap(int n) {
        int row = n >> 5;
        a[row] |= 1 << (n & 0x1F);

        // 移位的数字 移位符 移位次数
        // a[n / 32] |= 1 << n % 32;
    }
}
