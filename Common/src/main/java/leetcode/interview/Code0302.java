package leetcode.interview;

/**
 * 请设计一个栈，除了常规栈支持的pop与push函数以外，还支持min函数，该函数返回栈元素中的最小值。执行push、pop和min操作的时间复杂度必须为O(1)。
 * 示例：
 * MinStack minStack = new MinStack();
 * minStack.push(-2);
 * minStack.push(0);
 * minStack.push(-3);
 * minStack.getMin();   --> 返回 -3.
 * minStack.pop();
 * minStack.top();      --> 返回 0.
 * minStack.getMin();   --> 返回 -2.
 */
public class Code0302 {

    /**
     * 定义Node，并使用min来记录最小值，链表的head存储的min就是最小值
     */
    private static class Node {
        public int data;
        public int min;
        public Node next;

        Node(int i, int min) {
            this.data = i;
            this.min = min;
        }
    }

    private static Node head = null;

    // 进栈
    public static void push(int x) {
        if (head == null) {
            Node node = new Node(x, x);
            head = node;
        } else {
            int min = head.min > x ? x : head.min;
            Node node = new Node(x, min);
            node.next=head;
            head = node;
        }
    }

    // 出栈
    public static void pop() {
        if (head != null) {
            head = head.next;
        }
    }

    // 返回头部元素
    public static int top() {
        int n = 0;
        if (head != null) {
            n = head.data;
            head = head.next;
            return n;
        } else {
            return 0;
        }
    }
}
