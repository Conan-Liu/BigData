package leetcode.interview;

/**
 * 设计一个算法，找出二叉搜索树中指定节点的“下一个”节点（也即中序后继）。
 * 如果指定节点没有对应的“下一个”节点，则返回null。
 * 示例 1:
 * 输入: root = [2,1,3], p = 1
 * 2
 * / \
 * 1   3
 * 输出: 2
 * 示例 2:
 * 输入: root = [5,3,6,2,4,null,null,1], p = 6
 * 5
 * / \
 * 3   6
 * / \
 * 2   4
 * /
 * 1
 * 输出: null
 */
public class Code0406 {

    /**
     * 解题思路：中序遍历，并利用二叉搜索树的特性，左子树全部小于该节点，右子树全部大于该节点
     */

    // 该方法不大好理解
    public TreeNode inorderSuccessor(TreeNode root, TreeNode p) {
        // 记录最靠近p节点且大于p节点的那个节点
        TreeNode res = root;
        // 用于遍历
        TreeNode temp = root;
        while (temp != null) {
            if (temp.data <= p.data) {
                // 根绝二叉搜索树的特性，如果小于p.data，那么后继节点肯定在右子树上
                temp = temp.right;
            } else {
                // 如果大于p.data，那么可能是一个后继节点，但是还要查找是否有比这个节点更靠近p节点的，需要查找左子树
                res = temp;
                temp = temp.left;
            }
        }
        return res.data <= p.data ? null : res;
    }


    // 给出好理解的解决方法
    public TreeNode nextNode(TreeNode root, TreeNode p) {
        TreeNode res = null;
        if (p.right != null) {
            // 如果p存在右子树，则直接后继节点就是右子树的最左节点
            res = p.right;
            while (res.left != null) {
                res = res.left;
            }
            return res;
        } else {
            // 如果不存在右子树
            TreeNode node = root;
            while (p != node) {
                if (p.data < node.data) {
                    // 需要查找最靠近的节点
                    res = node;
                    node = node.left;
                } else {
                    node = node.right;
                }
            }
            return res;
        }
    }
}
