package com.conan.bigdata.common.bitmap;

/**
 * Created by Administrator on 2017/11/27.
 */
public class BitMap {
    public static int[] a = new int[1 + 10 / 32];

    public static void main(String[] args) {
        bitMap(10);
//        bitMap(33);
        System.out.println(a[0]);

        // 打印整数的二进制
        System.out.println(Integer.toBinaryString(10000000));
        System.out.println(Long.toBinaryString(5000000000L));

        // 二进制转十进制
        System.out.println(Integer.parseInt("100110001001011010000000",2));
        System.out.println(Long.parseLong("100101010000001011111001000000000",2));
    }

    public static void bitMap(int n) {
        int row = n >> 5;
        System.out.println("row = " + row);
        System.out.println("n & 0x1F = " + (n & 0x1F));
        a[row] |= 1 << (n & 0x1F);

        // 移位的数字 移位符 移位次数
        // a[n / 32] |= 1 << n % 32;
    }
}
