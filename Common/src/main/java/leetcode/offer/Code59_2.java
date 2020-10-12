package leetcode.offer;

import java.util.LinkedList;

/**
 * 队列的最大值
 * 请定义一个队列并实现函数 max_value 得到队列里的最大值，要求函数max_value、push_back 和 pop_front 的均摊时间复杂度都是O(1)。
 * 若队列为空，pop_front 和 max_value 需要返回 -1
 * 示例 1：
 * 输入:
 * ["MaxQueue","push_back","push_back","max_value","pop_front","max_value"]
 * [[],[1],[2],[],[],[]]
 * 输出: [null,null,null,2,1,2]
 * 示例 2：
 * 输入:
 * ["MaxQueue","pop_front","max_value"]
 * [[],[],[]]
 * 输出: [null,-1,-1]
 */
public class Code59_2 {

    class Node {
        int val;
        int max;
        Node next;
    }

    Node head = null;
    Node tail=null;
    public Code59_2() {

    }

    public int max_value() {
        if(head!=null){
            return head.max;
        }else{
            return -1;
        }
    }

    public void push_back(int value) {
        Node n1 = new Node();
        n1.val = value;
        n1.max = value;
        if (head == null) {
            head = n1;
        } else {
            if (n1.max < head.max) {
                int tmp = head.max;
                head.max = n1.max;
                n1.max = tmp;
            }
            n1.next = head;
            head = n1;
        }
    }

    public int pop_front() {
        if (head != null) {
            Node n = head;
            head = head.next;
            return n.val;
        } else {
            return -1;
        }
    }
}
