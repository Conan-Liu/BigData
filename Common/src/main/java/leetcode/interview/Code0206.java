package leetcode.interview;

import leetcode.interview.Tools.LeetNode;

/**
 * 编写一个函数，检查输入的链表是否是回文的。
 * 示例 1：
 * 输入： 1->2
 * 输出： false
 * 示例 2：
 * 输入： 1->2->2->1
 * 输出： true
 */
public class Code0206 {

    // 第一次遍历获取长度，第二遍历使用栈，时间复杂度O(n)，空间复杂度O(n)
    private static void m1() {

    }

    // 翻转链表，然后两个链表同时遍历，遇到不相同的节点，则不是回文链表，时间复杂度O(n)，空间复杂度O(n)
    private static void m2() {

    }

    // 寻找中间节点，然后反转前半部分或者后半部分，接着从头和从中间开始遍历，如果遇到不一样的节点，就不是回文
    private static boolean m3(LeetNode head) {
        LeetNode mid = searchMidNode(head);
        Tools.reverse(mid.next);
        boolean isSame = true;
        while (isSame && mid != null) {
            if (head.data != mid.data)
                isSame = false;
            head = head.next;
            mid = mid.next;
        }
        return isSame;
    }

    // 双指针查找中间点
    private static LeetNode searchMidNode(LeetNode head) {
        LeetNode p1 = head, p2 = head;
        while (head != null && p1.next != null && p2.next.next != null) {
            p1 = p1.next;
            p2 = p2.next.next;
        }
        return p1;
    }

    public static void main(String[] args) {
        LeetNode head = Tools.createLink(10);

    }
}
