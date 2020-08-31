package leetcode.interview;

/**
 * 实现一个函数，检查二叉树是否平衡。在这个问题中，平衡树的定义如下：任意一个节点，其两棵子树的高度差不超过 1。
 * 示例 1:
 * 给定二叉树 [3,9,20,null,null,15,7]
 * 3
 * / \
 * 9  20
 * /  \
 * 15   7
 * 返回 true
 * 示例 2:
 * 给定二叉树 [1,2,2,3,3,null,null,4,4]
 * 1
 * / \
 * 2   2
 * / \
 * 3   3
 * / \
 * 4   4
 * 返回 false
 */
public class Code0404 {

    private boolean flag = true;

    public boolean isBalanced(TreeNode root) {
        dfs(root);
        return flag;
    }

    private int dfs(TreeNode node) {
        if (node == null || !flag) {
            return 0;
        }
        int leftDepth = dfs(node.left) + 1;
        int rightDepth = dfs(node.right) + 1;
        if (leftDepth - rightDepth > 1 || rightDepth - leftDepth > 1)
            flag=false;
        return Math.max(leftDepth,rightDepth);
    }
}
