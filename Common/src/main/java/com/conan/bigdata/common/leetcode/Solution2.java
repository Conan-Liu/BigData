package com.conan.bigdata.common.leetcode;


/**
 * Created by Administrator on 2019/4/11.
 * 两数相加
 */
class ListNode {
    int val;
    ListNode next;

    ListNode(int x) {
        this.val = x;
    }
}

public class Solution2 {
    public static ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        ListNode head = null;
        ListNode sumFront = null;
        ListNode sumEnd;
        int g;
        int s = 0;
        while (l1 != null || l2 != null) {
            int num = (l1 == null ? 0 : l1.val) + (l2 == null ? 0 : l2.val) + s;
            g = num % 10;
            s = num / 10;
            sumEnd = new ListNode(g);
            if (sumFront == null) {
                sumFront = sumEnd;
            } else {
                sumFront.next = sumEnd;
                sumFront = sumEnd;
            }
            if (head == null) {
                head = sumEnd;
            }
            l1 = l1 == null ? null : l1.next;
            l2 = l2 == null ? null : l2.next;
        }
        if (s > 0) {
            sumEnd = new ListNode(s);
            sumFront.next = sumEnd;
        }
        return head;
    }

    public static ListNode initialListNode(int[] arr) {
        ListNode head = null;
        ListNode front = null;
        ListNode end;
        for (int a : arr) {
            end = new ListNode(a);
            if (front == null) {
                front = end;
            } else {
                front.next = end;
                front = end;
            }
            if (head == null) {
                head = end;
            }
        }
        return head;
    }

    public static void printListNode(ListNode l) {
        StringBuilder sb = new StringBuilder();
        while (l != null) {
            sb.append(l.val);
            l = l.next;
        }
        System.out.println("正数:" + Integer.valueOf(sb.reverse().toString()));
    }

    public static void main(String[] args) {
        ListNode l1 = initialListNode(new int[]{5});
        ListNode l2 = initialListNode(new int[]{5});
        printListNode(l1);
        printListNode(l2);
        ListNode listNode = addTwoNumbers(l1, l2);
        printListNode(listNode);
    }
}