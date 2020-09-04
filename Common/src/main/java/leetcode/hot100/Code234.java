package leetcode.hot100;

import java.util.HashMap;
import java.util.Map;

/**
 * 请判断一个链表是否为回文链表。
 * 示例 1:
 * 输入: 1->2
 * 输出: false
 * 示例 2:
 * 输入: 1->2->2->1
 * 输出: true
 * 可以快慢指针找到中间节点，然后对后半段链表反转
 * 最后两个指针一个从头开始，一个从中间位置开始，同时遍历，遇到不同的数字，则返回false
 */
public class Code234 {

    public boolean isPalindrome(ListNode head) {
        if (head == null || head.next == null) {
            return true;
        }

        ListNode slow = head, fast = head;
        ListNode pre = head, prepre = null;

        // 这里是核心，快慢指针向后遍历的同时，把慢指针走过的节点反转，也就是前半段数据反过来
        while (fast != null && fast.next != null) {
            pre = slow;
            slow = slow.next;
            fast = fast.next.next;
            pre.next = prepre;
            prepre = pre;
        }
        if (fast != null) {
            slow = slow.next;
        }

        // 对反过来的数据，前半段和后半段同时遍历，只要一个数字不一样，就可以返回false
        while (pre != null && slow != null) {
            if (pre.val != slow.val) {
                return false;
            }
            pre = pre.next;
            slow = slow.next;
        }
        return true;
    }
}
