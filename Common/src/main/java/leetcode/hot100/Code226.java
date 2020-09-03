package leetcode.hot100;

/**
 * 翻转一棵二叉树。
 * 示例：
 * 输入：
 * 4
 * /   \
 * 2     7
 * / \   / \
 * 1   3 6   9
 * 输出：
 * 4
 * /   \
 * 7     2
 * / \   / \
 * 9   6 3   1
 */
public class Code226 {

    public TreeNode invertTree(TreeNode root) {

        if(root==null){
            return root;
        }

        TreeNode left=root.left;
        TreeNode right=root.right;

        root.right=left;
        root.left=right;

        invertTree(root.left);
        invertTree(root.right);

        return root;
    }

}
