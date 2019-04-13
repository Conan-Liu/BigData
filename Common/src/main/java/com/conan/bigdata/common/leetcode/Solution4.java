package com.conan.bigdata.common.leetcode;

import java.util.Arrays;

/**
 * Created by Administrator on 2019/4/11.
 * 寻找两个有序数组的中位数
 */
public class Solution4 {
    public static double findMedianSortedArrays(int[] nums1, int[] nums2) {
        int i = 0, j = 0;
        int x = 0;
        int num[] = new int[nums1.length + nums2.length];
        // nums1  1 3 4 7
        // nums2  3 8
        while (i < nums1.length || j < nums2.length) {
            if (i == nums1.length) {
                num[x] = nums2[j];
                x++;
                j++;
                continue;
            }
            if (j == nums2.length) {
                num[x] = nums1[i];
                x++;
                i++;
                continue;
            }
            if (nums1[i] <= nums2[j]) {
                num[x] = nums1[i];
                x++;
                i++;
            } else {
                num[x] = nums2[j];
                x++;
                j++;
            }
        }
        System.out.println(Arrays.toString(num));

        int b = num.length - 1;
        int a = 0;
        while (a < b) {
            a++;
            b--;
        }
        if (a == b) {
            return num[a];
        } else {
            return (num[a] + num[b]) / 2.0;
        }
    }

    public static void main(String[] args) {
        System.out.println(findMedianSortedArrays(new int[]{1, 3}, new int[]{2}));
    }
}