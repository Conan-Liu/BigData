package leetcode.hot100;

/**
 * 给定一棵二叉树，你需要计算它的直径长度。一棵二叉树的直径长度是任意两个结点路径长度中的最大值。这条路径可能穿过也可能不穿过根结点。
 * 示例 :
 * 给定二叉树
 * 1
 * / \
 * 2   3
 * / \
 * 4   5
 * 返回 3, 它的长度是路径 [4,2,1,3] 或者 [5,2,1,3]。
 */
public class Code543 {

    int ans = 1;

    public int diameterOfBinaryTree(TreeNode root) {

        depth(root);
        return ans - 1;
    }

    public int depth(TreeNode node) {
        if (node == null) {
            return 0;
        }

        int L = depth(node.left);
        int R = depth(node.right);

        ans = Math.max(ans, L + R + 1);
        return Math.max(L, R) + 1;
    }
}
