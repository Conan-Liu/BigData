package leetcode.offer;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * 剑指 Offer 40. 最小的k个数
 * 输入整数数组 arr ，找出其中最小的 k 个数。例如，输入4、5、1、6、2、7、3、8这8个数字，则最小的4个数字是1、2、3、4。
 * 示例 1：
 * 输入：arr = [3,2,1], k = 2
 * 输出：[1,2] 或者 [2,1]
 * 示例 2：
 * 输入：arr = [0,1,2,1], k = 1
 * 输出：[0]
 */
public class Code40 {

    // 排序法
    public int[] getLeastNumbers(int[] arr, int k) {
        if (arr.length == 0) return new int[0];
        Arrays.sort(arr);
        return Arrays.copyOfRange(arr, 0, k);
    }

    // 大根堆求最小
    public int[] getLeastNumbers1(int[] arr, int k) {
        if (arr.length == 0) return new int[0];

        Queue<Integer> heap = new PriorityQueue<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return - o1.compareTo(o2);
            }
        });
        for(int num:arr){
            heap.offer(num);
            if(heap.size()>k){
                heap.poll();
            }
        }

        int[] nums=new int[k];
        int idx=0;
        for(Integer n:heap){
            nums[idx++]=n;
        }
        return nums;
    }
}
