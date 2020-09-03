package leetcode.hot100;

/**
 *
 */
public class Code581 {

    /**
     * 只需要考虑开头的数是不是最小的数且是递增的，末尾的数是不是最大的也是递增的，不满足这个规则的地方，就是需要排序的，即所求最小子数组
     * 双指针分别从头尾开始依次遍历，头指针越来越大，尾指针越来越小
     */
    public int findUnsortedSubarray(int[] nums) {

        int len=nums.length;
        int max=nums[0];
        int min=nums[len-1];
        int l=0,r=-1;

        for(int i=0;i<len;i++){
            // 对于右边界，如果不是增大的趋势，那么发生变化的这个下标就是边界，一定要遍历完
            if(max>nums[i]){
                r=i;
            }else{
                max=nums[i];
            }

            // 对于左边界，一定要是减少的趋势
            if(min<nums[len-i-1]){
                l=len-i-1;
            }else{
                min=nums[len-i-1];
            }
        }

        return r-l+1;
    }
}
