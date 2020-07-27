package com.conan.bigdata.common.algorithm;

import java.util.Arrays;

/**
 * 数组
 */
public class ArrayExp {

    private static int[] arr = {38, 65, 97, 76, 13, 27, 65, 49};

    /**
     * 寻找数组中最大值和最小值
     */
    private static void maxMin() {
        int len = arr.length;
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < len; i++) {
            if (arr[i] > max) {
                max = arr[i];
            }
            if (arr[i] < min) {
                min = arr[i];
            }
        }
        System.out.println(max + "," + min);
    }

    /**
     * 寻找数组中第二大的数
     */
    private static void secondMax() {
        int len = arr.length;
        int max = Integer.MIN_VALUE;
        int secondMax = Integer.MIN_VALUE;
        for (int i = 0; i < len; i++) {
            if (arr[i] > secondMax) {
                if (arr[i] > max) {
                    secondMax = max;
                    max = arr[i];
                } else if (arr[i] < max) {
                    secondMax = arr[i];
                }
            }
        }
        System.out.println(max + "," + secondMax);
    }

    /**
     * 求最大子数组之和
     */

    /**
     * 找出数组中重复元素最多的的数
     * 方Map里即可，通过map.put(arr[i], map.get(arr[i]) + 1)，然后遍历一遍map即可
     */

    /**
     * 求数组中两两相加等于20的组合
     * 1. 两层循环遍历，时间复杂度是O(n2)
     * 2. 先排序，选时间复杂度为O(nlogn)的排序算法，如堆排序，然后首尾两个下标遍历，arr[begin] + arr[end] < 20，则表示这两个数必然在[begin + 1, end]之间，> 20时，则在[begin, end - 1]之间
     */

    // 反转数组
    private static void reverse(int start, int end) {
        int temp;
        while (start < end) {
            temp = arr[start];
            arr[start] = arr[end];
            arr[end] = temp;
            start++;
            end--;
        }
        show();
    }

    /**
     * 把一个数组循环右移K位或左移K位
     * 可以把数组看成前后两部分，假设右移2位，12345678看成两部分123456 78，分别反转654321 87，然后作为整体反转78 123456
     */
    private static void shiftK() {
        int k = 2;
        k = k % arr.length; // 防止k比n大
        int len = arr.length;
        reverse(0, len - k - 1);
        reverse(len - k, len - 1);
        reverse(0, len - 1);
    }

    /**
     * 打印数组元素的排列情况
     * TODO...
     */

    public static void main(String[] args) {
        // maxMin();
        // secondMax();
        // reverse(0, arr.length - 1);
        shiftK();
    }

    private static void show() {
        System.out.println(Arrays.toString(arr));
    }
}
