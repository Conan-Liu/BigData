package leetcode.offer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 剑指 Offer 32 - I. 从上到下打印二叉树
 * 从上到下打印出二叉树的每个节点，同一层的节点按照从左到右的顺序打印。
 * 例如:
 * 给定二叉树: [3,9,20,null,null,15,7],
 * 3
 * / \
 * 9  20
 * /  \
 * 15   7
 * 返回：
 * [3,9,20,15,7]
 */
public class Code32 {

    public int[] levelOrder(TreeNode root) {
        if (root == null) {
            return new int[0];
        }

        List<TreeNode> list = new ArrayList<>();
        list.add(root);
        int i = 0;
        while (true) {
            TreeNode tmp = list.get(i);
            i++;
            if (tmp.left != null)
                list.add(tmp.left);
            if (tmp.right != null)
                list.add(tmp.right);
            if (i == list.size()) {
                break;
            }
        }

        int[] nums = new int[list.size()];
        int j = 0;
        for (TreeNode node : list) {
            nums[j++] = node.val;
        }
        return nums;
    }


    public List<List<Integer>> levelOrder1(TreeNode root) {
        List<List<Integer>> lists = new ArrayList<>();
        Queue<TreeNode> tree = new LinkedBlockingQueue<>();
        if (root != null)
            tree.add(root);
        while (!tree.isEmpty()) {
            List<Integer> list = new ArrayList<>();
            int size = tree.size();
            for (int i = 0; i < size; i++) {
                TreeNode node = tree.poll();
                list.add(node.val);

                if (node.left != null)
                    tree.add(node.left);
                if (node.right != null)
                    tree.add(node.right);
            }
            lists.add(list);
        }
        return lists;
    }

}
