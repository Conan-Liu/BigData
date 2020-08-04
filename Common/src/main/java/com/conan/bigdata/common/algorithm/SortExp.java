package com.conan.bigdata.common.algorithm;

import java.util.Arrays;

/**
 * 排序算法
 * 以下实例为升序
 */
public class SortExp {

    private static int[] arr = {38, 65, 97, 76, 13, 27, 65, 49};

    public static void main(String[] args) {
        System.out.println(Arrays.toString(arr));
        // bubbleSort();
        selectSort();
    }


    /**
     * 冒泡排序
     * 一次比较相邻的两个元素，比较后如果第一个比第二个大，则交换数据，否则不交换，依次向后遍历，第一轮得到最后一位是最大的数
     * 这样经过多轮以后，排序完成
     * 时间复杂度 O(n2），空间复杂度 O(1)，稳定排序
     */
    private static void bubbleSort() {
        int len = arr.length;
        int temp;
        for (int i = 0; i < len; i++) {
            for (int j = 0; j < len - i - 1; j++) {
                if (arr[j] > arr[j + 1]) {
                    temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                }
            }
        }
        show();
    }

    /**
     * 选择排序
     * 每一轮选择一个最小或最大的数来和其实位置交换，也可以是末尾位置，每一轮确定一个数，多轮后，排序完成
     * 时间复杂度 O(n2），空间复杂度 O(1)，不稳定排序
     */
    private static void selectSort() {
        int len = arr.length;
        int minIndex;
        int temp;
        for (int i = 0; i < len; i++) {
            minIndex = i;
            for (int j = i + 1; j < len; j++) {
                if (arr[minIndex] > arr[j]) {
                    minIndex = j;
                }
            }
            temp = arr[minIndex];
            arr[minIndex] = arr[i];
            arr[i] = temp;
        }
        show();
    }

    /**
     * 插入排序
     * 假设第一个元素已经有序，从第二个开始，从后向前遍历，遇到大于该元素，就向后移动，直到不大于，就把该元素插入此处
     */
    public static void insertSort() {
        int len = arr.length;
        for (int i = 1; i < len; i++) {
            int current = arr[i];
            for (int j = i; j > 0; j--) {
                if (current >= arr[j - 1]) {
                    arr[j]=current;
                    break;
                }
                arr[j] = arr[j - 1];
            }
        }
        show();
    }

    private static void show(){
        System.out.println(Arrays.toString(arr));
    }
}
