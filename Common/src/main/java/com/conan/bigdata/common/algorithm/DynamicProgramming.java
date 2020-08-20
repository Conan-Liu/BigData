package com.conan.bigdata.common.algorithm;

import java.util.HashMap;
import java.util.Map;

/**
 * 动态规划
 * 三个重要概念
 * 最优子结构
 * 边界
 * 状态转移公式
 */
public class DynamicProgramming {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        System.out.println(fDp(47));
        long end = System.currentTimeMillis();
        System.out.println("time : " + (end - start));
    }

    /**
     * 有一座高度是10级台阶的楼梯，从下往上走，每跨一步只能向上1级或者2级台阶。要求用程序来求出一共有多少种走法。
     * 一次可以走1级或者2级，假设0到8级台阶总共X种走法，0到9级台阶总共Y种走法，那么0到10级台阶就是X+Y种走法，记为F(10)=F(9)+F(8)
     * 同理F(9)=F(8)+F(7)，得到通用表达式F(n)=F(n-1)+F(n-2)，当只有1级台阶时，就1种走法，2级台阶就2种走法
     * 如此分析典型的递归，但是这个时间复杂度高，O(2^n)，指数上升，因为部分数据重复很多遍计算
     * F(9)和F(8)是F(10)的最优子结构
     * F(1)和F(2)是问题的边界，表示可以返回，递归里面就是递归终止条件，不能无限递归
     * F(n)=F(n-1)+F(n-2)，就是状态转移公式，可以理解为归纳表达式
     */
    private static int f(int n) {
        if (n < 0)
            return 0;
        if (n == 1)
            return 1;
        if (n == 2)
            return 2;

        // 这种两个递归方法的时间复杂度，近似于二叉树，O(2^n)2的n次方
        int fn1 = f(n - 1);
        int fn2 = f(n - 2);
        return fn1 + fn2;
    }

    /**
     * 针对上面算法优化计算，针对数据重复计算的问题，可以使用HashMap把F(n)保存起来，省去重复计算，时间复杂度O(n)，空间复杂度O(n)
     */
    private static final Map<Integer, Integer> map = new HashMap<>();

    private static int fMap(int n) {
        if (n < 0)
            return 0;
        if (n == 1)
            return 1;
        if (n == 2)
            return 2;

        if (map.containsKey(n)) {
            return map.get(n);
        } else {
            int fn1 = fMap(n - 1);
            int fn2 = fMap(n - 2);
            int value = fn1 + fn2;
            map.put(n, value);
            return value;
        }
    }

    /**
     * 接下来使用动态规划继续优化
     * 时间复杂度已经不可继续优化了，空间复杂度可以优化
     * 我们可以看到F(n)=F(n-1)+F(n-2)，那么意味着每个台阶数，只与前两个台阶数相关，可以循环从底向上累加
     * 时间复杂度O(n)，空间复杂度O(1)
     */
    private static int fDp(int n){
        if (n < 0)
            return 0;
        if (n == 1)
            return 1;
        if (n == 2)
            return 2;

        int a=1;
        int b=2;
        int temp=0;
        for(int i=3;i<=n;i++){
            temp=a+b;
            a=b;
            b=temp;
        }
        return temp;
    }

}
