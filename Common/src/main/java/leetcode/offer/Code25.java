package leetcode.offer;

/**
 * 合并两个排序的链表
 * 输入两个递增排序的链表，合并这两个链表并使新链表中的节点仍然是递增排序的。
 * 示例1：
 * 输入：1->2->4, 1->3->4
 * 输出：1->1->2->3->4->4
 */
public class Code25 {

    public ListNode mergeTwoLists(ListNode l1, ListNode l2) {
        ListNode head = null, tail = null;
        if (l1 == null && l2 == null) {
            return null;
        } else if (l1 == null) {
            head = l2;
            l2 = null;
        } else if (l2 == null) {
            head = l1;
            l1 = null;
        } else {
            if (l1.val <= l2.val) {
                head = l1;
                tail = head;
                l1 = l1.next;
            } else {
                head = l2;
                tail = head;
                l2 = l2.next;
            }
        }
        while (l1 != null || l2 != null) {
            if (l1 == null) {
                tail.next = l2;
                break;
            }
            if (l2 == null) {
                tail.next = l1;
                break;
            }

            if (l1.val <= l2.val) {
                tail.next = l1;
                l1 = l1.next;
                tail = tail.next;
            } else {
                tail.next = l2;
                l2 = l2.next;
                tail = tail.next;
            }
        }
        return head;
    }

    // 可以使用一个头节点，方便操作
    public ListNode mergeTwoLists1(ListNode l1, ListNode l2) {
        if (l1 == null || l2 == null) {
            return l1 == null ? l2 : l1;
        }

        ListNode head = new ListNode(-1);
        ListNode tail = head;
        while (l1 != null && l2 != null) {
            if (l1.val <= l2.val) {
                tail.next = l1;
                l1 = l1.next;
            } else {
                tail.next = l2;
                l2 = l2.next;
            }
            tail = tail.next;
        }
        if(l1==null){
            tail.next=l2;
        }else{
            tail.next=l1;
        }
        return head.next;
    }

}
