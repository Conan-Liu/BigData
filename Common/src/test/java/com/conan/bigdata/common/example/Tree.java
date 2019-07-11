package com.conan.bigdata.common.example;

import org.junit.Test;

/**
 * Created by Administrator on 2019/7/9.
 */
class TreeNode {
    int val;
    TreeNode left;
    TreeNode right;

    public TreeNode(int x) {
        this.val = val;
    }
}

public class Tree {

    private TreeNode createBST(int[] nums, int low, int high) {
        if (high < low) {
            return null;
        }
        int mid = (low + high) >> 1;
        TreeNode tNode = new TreeNode(nums[mid]);
        tNode.left = createBST(nums, low, mid - 1);
        tNode.right = createBST(nums, mid + 1, high);
        return tNode;
    }

    /**
     * 有序数组生成二叉搜索树
     */
    @Test
    public void sortedArrayToBST(){
        int[] nums={-10,-4,-1,0,4,11,22,100,1024};
        int low=0,high=nums.length-1;
        TreeNode tree=createBST(nums,low,high);
    }
}