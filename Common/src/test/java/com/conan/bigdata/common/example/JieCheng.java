package com.conan.bigdata.common.example;

import org.junit.Test;

/**
 * Created by Administrator on 2019/4/11.
 */
public class JieCheng {

    /**
     * 计算 1 + 1/2! + 1/3! + .. + 1/20!
     */
    @Test
    public void test1() {
        double sum = 0;
        for (int i = 1; i <= 20; i++) {
            int jiecheng = 1;
            for (int j = 1; j <= i; j++) {
                jiecheng *= j;
            }
            sum += 1.0 / jiecheng;
        }
        System.out.println(sum);
    }
}