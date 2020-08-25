package leetcode.interview;

import leetcode.interview.Tools.LeetNode;

import java.util.HashSet;
import java.util.Set;

/**
 * 编写代码，移除未排序链表中的重复节点。保留最开始出现的节点。
 * 示例1:
 * 输入：[1, 2, 3, 3, 2, 1]
 * 输出：[1, 2, 3]
 * 示例2:
 * 输入：[1, 1, 1, 1, 2]
 * 输出：[1, 2]
 */
public class Code0201 {

    // 两层循环，时间复杂度O(n^2)，空间复杂度O(1)
    private static LeetNode removeDuplicateNodes(LeetNode head) {
        if(head==null){
            return null;
        }
        LeetNode target = head;
        while (target != null) {
            LeetNode p1 = target;
            LeetNode p2 = target.next;
            while (p2 != null) {
                if (target.data == p2.data) {
                    p1.next = p2.next;
                    p2 = p2.next;
                } else {
                    p1 = p1.next;
                    p2 = p2.next;
                }
            }
            target = target.next;
        }
        return head;
    }

    // 一遍循环，时间复杂度O(n)，空间复杂度O(n)
    private static LeetNode remove1(LeetNode head) {
        if(head==null){
            return null;
        }
        LeetNode p = head;
        Set<Integer> set = new HashSet<>();
        set.add(p.data);
        while (p.next != null) {
            if (set.contains(p.next.data)) {
                p.next = p.next.next;
            } else {
                set.add(p.next.data);
                p = p.next;
            }
        }
        return head;
    }

    public static void main(String[] args) {
        LeetNode head = Tools.createLink();
        Tools.show(head);
        remove1(head);
        Tools.show(head);
    }
}
