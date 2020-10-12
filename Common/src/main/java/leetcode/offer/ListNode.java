package leetcode.offer;

public class ListNode {

    public int val;

    public ListNode next;

    public ListNode(int val) {
        this.val = val;
    }

    public ListNode(int val, ListNode listNode){
        this.val=val;
        this.next=listNode;
    }
}
