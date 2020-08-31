package com.conan.bigdata.common.algorithm;

/**
 * 二叉树套路编程
 * 二叉树算法的设计的总路线：明确一个节点要做的事情，然后剩下的事抛给框架。
 * void traverse(TreeNode root) {
 * // root 需要做什么？在这做。
 * // 其他的不用 root 操心，抛给框架来处理左右子树
 * traverse(root.left);
 * traverse(root.right);
 * }
 * BST 二叉查找树，二叉搜索树套路编程
 * 下面举例子
 */
public class BST {

    private class TreeNode {
        int data;
        TreeNode left;
        TreeNode right;

        TreeNode(int i) {
            this.data = i;
        }
    }

    /**
     * 给树所有节点的值加1
     */
    public void plusOne(TreeNode root) {
        // root 需要做什么？其它先不要管：root需要判断是否为null，且给root加1
        if (root == null)
            return;
        root.data += 1;

        // 左右子树当作一个新的树，重新调用该方法，递归
        plusOne(root.left);
        plusOne(root.right);
    }

    /**
     * 判断二叉树是否完全相同
     */
    public boolean isSame(TreeNode p, TreeNode q) {
        // root需要做什么？其它先不要管：判断两个root节点是否相同
        if (p == null && q == null)
            return true;
        if (p == null || q == null)
            return false;
        if (p.data != q.data)
            return false;

        // 左右子树当作新的树，递归调用该方法，左右子树都相同，则表示整棵树相同
        return isSame(p.left, q.left) && isSame(p.right, q.right);
    }

    /**
     * 判断二叉搜索树的合法性
     * 当然这是不正确的，少考虑了一点，以后补充
     */
    public boolean isValidBST(TreeNode root) {
        // root需要做什么？
        if (root == null)
            return true;
        if (root.left != null && root.left.data >= root.data)
            return false;
        if (root.right != null && root.right.data <= root.data)
            return false;

        // 左右子树当作新的树，递归调用
        return isValidBST(root.left) && isValidBST(root.right);
    }

    /**
     * 在二叉树中查找一个数是否存在
     * 二叉搜索树也是一棵二叉树，照样可以使用这种方法，继续思考：在二叉搜索树时，如果利用它的特性更快的查找
     */
    public boolean isInTree(TreeNode root, int target) {
        // root需要做什么？直接判断data是否就是这个数
        if (root == null)
            return false;
        if (root.data == target)
            return true;

        // 递归左右子树
        return isInTree(root.left, target) && isInTree(root.right, target);
    }

    /**
     * 在二叉搜索树中查找数的存在
     * 利用左子树小，右子树大的特性修改上面的代码
     */
    public boolean isInBST(TreeNode root, int target) {
        // root要做什么？ 直接判断root节点是否是这个数
        if (root == null)
            return false;
        if (root.data == target)
            return true;

        // 左右子树需要利用二叉搜索树的特性递归，跟root判断大小后，只需要递归一边的子树
        if (root.data > target) {
            return isInBST(root.left, target);
        }
        if (root.data < target) {
            return isInBST(root.right, target);
        }

        // 这里的return无意义，因为代码永远不会到执行这里，只是为了语法正确
        return false;
    }
}
