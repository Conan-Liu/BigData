package com.conan.bigdata.common.algorithm;

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

    public static void main(String[] args) {
        // maxMin();
        secondMax();
    }
}
