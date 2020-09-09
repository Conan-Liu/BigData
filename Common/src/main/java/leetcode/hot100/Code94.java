package leetcode.hot100;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * 94. 二叉树的中序遍历
 * 给定一个二叉树，返回它的中序 遍历。
 * 示例:
 * 输入: [1,null,2,3]
 * 1
 * \
 * 2
 * /
 * 3
 * 输出: [1,3,2]
 */
public class Code94 {

    // 递归的方式
    public List<Integer> inorderTraversal(TreeNode root) {
        List<Integer> res = new ArrayList<>();
        helper(root, res);
        return res;
    }

    public void helper(TreeNode root, List<Integer> res) {
        if (root != null) {
            helper(root.left, res);
            res.add(root.val);
            helper(root.right, res);
        }
    }


    // 迭代的方式
    // 需要利用栈，遍历左中右
    public List<Integer> inorderTraversal1(TreeNode root) {
        List<Integer> res = new ArrayList<>();
        Stack<TreeNode> temp=new Stack<>();
        while (root!=null||temp.size()>0){
            if(root!=null){
                temp.add(root);
                root=root.left;
            }else{
                TreeNode right=temp.pop();
                res.add(right.val);
                root=root.right;
            }
        }
        return res;
    }
}
