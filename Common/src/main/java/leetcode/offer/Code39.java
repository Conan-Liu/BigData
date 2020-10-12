package leetcode.offer;

import java.util.Arrays;

/**
 * 剑指 Offer 39. 数组中出现次数超过一半的数字
 * 数组中有一个数字出现的次数超过数组长度的一半，请找出这个数字。
 * 你可以假设数组是非空的，并且给定的数组总是存在多数元素。
 * 示例 1:
 * 输入: [1, 2, 3, 2, 2, 2, 5, 4, 2]
 * 输出: 2
 */
public class Code39 {

    public int majorityElement(int[] nums) {
        Arrays.sort(nums);
        return nums[nums.length / 2];
    }


    public int majorityElement1(int[] nums) {
        int cur = 0, count = 0;
        for (int n : nums) {
            if (count == 0)
                cur = n;
            count += (n == cur ? 1 : -1);
        }
        return cur;
    }
}
