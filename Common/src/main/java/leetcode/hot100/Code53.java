package leetcode.hot100;

/**
 * 给定一个整数数组 nums ，找到一个具有最大和的连续子数组（子数组最少包含一个元素），返回其最大和。
 * 示例:
 * 输入: [-2,1,-3,4,-1,2,1,-5,4]
 * 输出: 6
 * 解释: 连续子数组 [4,-1,2,1] 的和最大，为 6。
 * 进阶:
 * 如果你已经实现复杂度为 O(n) 的解法，尝试使用更为精妙的分治法求解。
 */
public class Code53 {

    // 时间复杂度O(n^2)
    public static int maxSubArray(int[] nums) {
        int max = Integer.MIN_VALUE;
        for (int i = 0; i < nums.length; i++) {
            int sum = nums[i];
            if (sum > max)
                max = sum;
            for (int j = i + 1; j < nums.length; j++) {
                sum += nums[j];
                if (sum > max)
                    max = sum;
            }
        }
        return max;
    }

    public static int maxSubArray1(int[] nums) {
        int ans = nums[0];
        int sum = 0;
        for(int num: nums) {
            if(sum > 0) {
                sum += num;
            } else {
                sum = num;
            }
            ans = Math.max(ans, sum);
        }
        return ans;
    }

    public static void main(String[] args) {
        int[] nums=new int[]{-1,-2};
        System.out.println(maxSubArray1(nums));
    }
}
