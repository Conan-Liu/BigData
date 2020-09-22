package leetcode.offer;

import java.util.LinkedList;

/**
 * 请完成一个函数，输入一个二叉树，该函数输出它的镜像。
 * 例如输入：
 *      4
 *    /   \
 *   2     7
 *  / \   / \
 * 1   3 6   9
 * 镜像输出：
 *      4
 *    /   \
 *   7     2
 *  / \   / \
 * 9   6 3   1
 */
public class Code27 {

    // 递归调用
    public TreeNode mirrorTree(TreeNode root) {
        if (root == null) {
            return root;
        }

        TreeNode tmp = root.left;
        root.left = root.right;
        root.right = tmp;

        mirrorTree(root.left);
        mirrorTree(root.right);
        return root;
    }

    public TreeNode mirrorTree1(TreeNode root) {
        if (root == null) {
            return root;
        }

        TreeNode left = root.left;
        TreeNode right = root.right;

        root.right = mirrorTree(root.left);
        root.left = mirrorTree(root.right);
        return root;
    }

    // 迭代调用
    public TreeNode mirrorTree2(TreeNode root) {
        if (root == null) {
            return root;
        }

        LinkedList<TreeNode> list = new LinkedList<>();
        list.offer(root);
        while (list.size() > 0) {
            TreeNode r = list.pop();
            TreeNode tmp = r.left;
            r.left = r.right;
            r.right = tmp;

            if (r.left != null)
                list.offer(r.left);
            if (r.right != null)
                list.offer(r.right);
        }
        return root;
    }
}
