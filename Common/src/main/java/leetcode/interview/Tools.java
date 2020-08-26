package leetcode.interview;

import java.util.Random;

/**
 *
 */
public class Tools {

    public static class LeetNode{
        public int data;
        public LeetNode next;
        public LeetNode(int i){
            this.data=i;
        }
    }

    // 0201 创建链表
    public static LeetNode createLink(int n){
        LeetNode head=null;
        LeetNode p;
        Random r=new Random();
        for(int i=0;i<n;i++){
            p=new LeetNode(r.nextInt(8));
            if(head==null){
                head=p;
            }else{
                p.next=head;
                head=p;
            }
        }
        return head;
    }

    public static void show(LeetNode head){
        LeetNode node=head;
        System.out.println("********************");
        while (node!=null){
            System.out.print(node.data+" ");
            node=node.next;
        }
        System.out.println();
    }

    public static void main(String[] args) {
        LeetNode node=createLink(10);
        show(node);
    }
}
