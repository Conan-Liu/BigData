package com.conan.bigdata.common.algorithm;

import java.util.concurrent.TimeUnit;

/**
 * 位运算
 */
public class BitExp {

    /**
     * 判断一个数是否为2的n次方
     * 1. 用1做左移位操作，看与给定的数是否相等，要注意大于该数时退出循环，时间复杂度O(logn)
     * 2. 2的n次方数转成二进制是1，10，100...，该数减1后是0，01，011...，可以看到这两个二进制的与为0，时间复杂度为O(1)
     */
    private static void bit2n() {
        // 1
        int n = 16;
        int i = 1;
        while (i <= n) {
            if (i == n) {
                System.out.println("yes");
            }
            // i 左移一位
            i<<=1;
        }
        System.out.println("no");

        // 2
        boolean is2 = (n & (n - 1)) == 0 ? true : false;
        System.out.println(is2);
    }

    /**
     * 求一个二进制数中1的个数
     * 1. 除2余1，则个数加1，最后为0
     * 2. 与上1，如果等于1，则表示个位为1，加1，然后该数右移一位
     */
    private static void bitCnt() throws InterruptedException {
        int n=-10;
        int cnt=0;
        while (n!=0){
            if((n&1)==1){
                cnt++;
            }
            n>>>=1;
        }
        System.out.println(cnt);
    }

    public static void main(String[] args) throws InterruptedException {
        // bit2n();
        bitCnt();
    }
}
