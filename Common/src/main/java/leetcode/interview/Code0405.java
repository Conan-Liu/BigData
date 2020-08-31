package leetcode.interview;

/**
 * 实现一个函数，检查一棵二叉树是否为二叉搜索树。
 * 二叉搜索树定义：根节点的值大于其左子树中任意一个节点的值，小于其右节点中任意一节点的值，这一规则适用于二叉查找树中的每一个节点
 * 示例 1:
 * 输入:
 * 2
 * / \
 * 1   3
 * 输出: true
 * 示例 2:
 * 输入:
 * 5
 * / \
 * 1   4
 * / \
 * 3   6
 * 输出: false
 * 解释: 输入为: [5,1,4,null,null,3,6]。
 *      根节点的值为 5 ，但是其右子节点值为 4
 */
public class Code0405 {

    public boolean isValidBST(TreeNode root) {
        if (root == null) {
            return true;
        }
        // 这里取long型，是为了避免Integer.MIN_VALUE，Integer.MAX_VALUE溢出，无法比较
        // 解释：左子树必须全部小于root.data，右子树必须全部大于root.data，同时满足条件
        return helper(root.left, Long.MIN_VALUE, root.data) && helper(root.right, root.data, Long.MAX_VALUE);
    }

    private boolean helper(TreeNode root, long min, long max) {
        if (root == null) {
            return true;
        }
        if (root.data <= min || root.data >= max)
            return false;

        boolean left = helper(root.left, min, root.data);
        boolean right = helper(root.right, root.data, max);
        return left && right;
    }
}
