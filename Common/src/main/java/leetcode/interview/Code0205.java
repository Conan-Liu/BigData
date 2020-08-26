package leetcode.interview;

import leetcode.interview.Tools.LeetNode;

/**
 * 给定两个用链表表示的整数，每个节点包含一个数位。
 * 这些数位是反向存放的，也就是个位排在链表首部。
 * 编写函数对这两个整数求和，并用链表形式返回结果。
 * 示例：
 * 输入：(7 -> 1 -> 6) + (5 -> 9 -> 2)，即617 + 295
 * 输出：2 -> 1 -> 9，即912
 * 进阶：假设这些数位是正向存放的，请再做一遍。
 * 示例：
 * 输入：(6 -> 1 -> 7) + (2 -> 9 -> 5)，即617 + 295
 * 输出：9 -> 1 -> 2，即912
 */
public class Code0205 {

    // 同时遍历两个链表，累加每个节点的数字，并加上进位
    private static LeetNode addTwoNumbers(LeetNode n1, LeetNode n2) {
        LeetNode head = null, tail = null;
        if (n1 == null)
            return n2;
        if (n2 == null) {
            return n1;
        }
        boolean upFlag = false;
        while (n1 != null || n2 != null) {
            int sum = 0;
            if (n1 != null) {
                sum += n1.data;
                n1 = n1.next;
            }
            if (n2 != null) {
                sum += n2.data;
                n2 = n2.next;
            }
            if (upFlag) {
                sum += 1;
                upFlag = false;
            }
            if (sum >= 10) {
                upFlag = true;
            }
            LeetNode node = new LeetNode(sum % 10);
            if (head == null) {
                head = tail = node;
            } else {
                tail.next = node;
                tail = tail.next;
            }
        }
        if (upFlag) {
            tail.next = new LeetNode(1);
        }
        return head;
    }

    public static void main(String[] args) {
        LeetNode n1 = Tools.createLink(4);
        LeetNode n2 = Tools.createLink(4);
        LeetNode head = addTwoNumbers(n1, n2);
        Tools.show(n1);
        Tools.show(n2);
        Tools.show(head);

    }
}
