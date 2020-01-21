package com.conan.bigdata.common.algorithm;

import java.util.Arrays;

/**
 * 字符串匹配算法有很多
 * BF      暴力匹配，一个字符一个字符的匹配，匹配不上则后移一位继续匹配，简单容易理解，时间复杂度O(MN)
 * KMP     名气大，实力一般般
 * BM      Boyer-Moore 使用比较多，各种记事本的“查找”功能(CTRL + F)一般都是采用的此算法
 * Sunday  比BM算法还要快，而且更容易理解的算法
 */
public class StringMatch {

//    private static final String STR = "这是字符串比较算法，包括BF,BM,Sunday，一个一个的举例子";
    private static final String STR = "AAAAAAAAAAAA";
//    private static final String PATTERN = "子";
    private static final String PATTERN = "a";

    public static void main(String[] args) {

        System.out.println("*************** BF 算法示例 **************************");
        bf();

        System.out.println("*************** BM 算法示例 **************************");
        bm();

        System.out.println("*************** Sunday 算法示例 **************************");
        sunday();

    }

    /**
     * BF 算法
     * 每次只移动一位， 效率低下
     */
    private static void bf() {
        String pattern = PATTERN.toUpperCase();
        // String 类 contains 方法内部调用indexOf，就是暴力匹配的
        if (STR.contains(pattern.toUpperCase())) {
            System.out.println("已找到");
        } else {
            System.out.println("未找到");
        }

        char[] sources = STR.toCharArray();
        System.out.println("sources 长度: " + sources.length);
        char[] patterns = pattern.toCharArray();
        System.out.println("patterns 长度: " + patterns.length);
        int i = 0;
        while (i <= sources.length - patterns.length) {
            int j = 0;
            while (j < patterns.length && sources[i + j] == patterns[j]) {
                j++;
            }
            if (j == patterns.length) {
                break;
            }
            i++;
        }
        if (i > sources.length - patterns.length) {
            System.out.println("未找到");
        } else {
            System.out.println("匹配上，索引下标 : " + i);
        }
    }

    /**
     * BM 算法
     */
    private static void bm() {
        String[] strings = "1个人".split("");
        System.out.println(Arrays.toString(strings));
    }

    /**
     * Sunday 算法
     */
    private static void sunday() {

    }
}