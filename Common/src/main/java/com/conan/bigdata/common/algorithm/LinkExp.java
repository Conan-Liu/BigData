package com.conan.bigdata.common.algorithm;

import lombok.Data;


/**
 * 链表的相关操作
 * 单链表和双链表
 * 双链表相对而言简单点
 * 以下代码以单链表为主，链表可以参考LinkedList，内部维护一个size，避免遍历时需要先计算长度
 * 可以维护一个头指针和尾指针方便操作
 */
public class LinkExp {

    @Data
    private static class Node {
        private int data;
        public Node next;

        public Node(int data) {
            this.data = data;
        }
    }

    // 链表存储的数据可以使用泛型，更灵活
    @Data
    private static class Node1<E> {
        private E data;
        public Node1<E> next;

        public Node1(E data) {
            this.data = data;
        }
    }


    public static void main(String[] args) {


//         show(create());

//         searchKElement(create(), 9);

//         show(reverse(create()));

        printReverse(create());

//        searchMid(create());

//        isLoop(create());

//        deleteNode(create());

//        isIntersect(create(),create());
    }

    private static Node create() {
        Node head = null;
        Node p = null;
        Node n;
        for (int i = 0; i < 11; i++) {
            n = new Node(i);
            if (i == 0) {
                head = n;
                p = n;
                continue;
            }
            p.next = n;
            p = n;
        }
        return head;
    }

    /**
     * 链表增删改查，省略...
     */
    private static void show(Node node) {
        while (node != null) {
            System.out.println(node.getData());
            node = node.next;
        }
    }

    /**
     * 查找链表的倒数第K个元素
     * 1. 两层循环 O(n平方)复杂度，先获得链表长度，然后从头遍历n-k个元素
     * 2. 从头开始遍历，遍历K个元素，如果是结尾，则就是这个数，如果不是则从第二个元素开始遍历，以此类推 O(kn)
     * 3. 维护两个指针，第一个先遍历K个元素，然后两个指针同时向后遍历，第一个指针到结尾后，第二个指针就是对应的第K个元素，O(k+n)
     * 下面是第3中方法的实现
     */
    private static void searchKElement(Node node, int k) {
        // node 作为第一个指针向后遍历， p1作为第K个元素的指针
        Node p1 = node;
        int i = 0;
        while (node != null) {
            if (i >= k) {
                p1 = p1.next;
            }
            node = node.next;
            i++;
        }
        System.out.println(p1.getData());
    }

    // 反转链表
    private static Node reverse(Node node) {
        Node head = null;
        Node p;
        while (node != null) {
            p = node.next;
            node.next = head;
            head = node;
            node = p;
        }
        return head;
    }

    /**
     * 从尾到头输出链表
     * 1. 可以考虑反转然后顺序打印
     * 2. 也可以使用栈
     * 3. 可以递归
     */
    private static void printReverse(Node node) {
        if (node.next != null) {
            printReverse(node.next);
        }
        System.out.println(node.getData());
    }

    /**
     * 链表两两反转
     */
    private static void doubleReverse(Node node) {

    }

    /**
     * 寻找链表的中间节点
     * 1. 遍历得到长度，然后取中间节点
     * 2. 可以使用两个指针，第一个指针一次走两步，第二个一次走一步，等第一个指针走到尾部，第二个刚好在中间
     */
    private static void searchMid(Node node) {
        // node作为第一个指针，p1作为第二个
        Node p1 = node;
        while (node != null && node.next != null && node.next.next != null) {
            p1 = p1.next;
            node = node.next.next;
        }
        System.out.println(p1.getData());
    }

    /**
     * 检测链表是否有环
     * 定义两个指针，第一个指针一次走两步，第二个一次走一步，如果两个指针最后重合了就代表有环，如果第一个指针先到达尾部Null，则无环
     * 设相遇点为node1，相遇点与环路起点的距离k = head与环路起点的距离k。
     * 用一个指针指向 head，另一个指针指点 node，以同样的速度移动k步之后，两者会指向环路起点。
     */
    private static void isLoop(Node node) {
        // p1作为快指针，p2为慢指针
        Node p1 = node, p2 = node;
        while (p1 != null && p1.next != null) {
            p1 = p1.next.next;
            p2 = p2.next;
            if (node == p1) {
                System.out.println("isLoop");
                break;
            }
        }
        if (node == null || node.next == null)
            System.out.println("noLoop");

        Node slow = node;
        while (slow != p2) {
            slow = slow.next;
            p2 = p2.next;
        }
    }

    /**
     * 不知道头指针的情况下，删除指定节点
     * 1. 链表尾节点，无法删除，因为无法更新前驱节点的next值
     * 2. 链表其它节点，可以通过交换该删除节点和后继节点的值，达到删除该节点的效果
     */
    private static void deleteNode(Node node) {
        // 随便假设一个节点要删除
        Node deleteNode = node.next.next.next;

        // 交换该节点和后继节点的值
        deleteNode.setData(deleteNode.next.getData());
        deleteNode.next = deleteNode.next.next;

        show(node);
    }

    /**
     * 判断两个链表是否相交，并找到相交的节点
     * 第一步：两个链表如果相交，那么必然有共同的尾节点
     * 第二部：在确定相交的同时记录下每个链表的长度，可以计算两个链表的长度差diff，这样长的那个链表先遍历diff后，两个链表同步遍历，遇到两个引用相同时，该节点就是相交点
     */
    private static boolean isIntersect(Node node1, Node node2) {
        if (node1 == null || node2 == null) {
            return false;
        }
        int len1 = 1, len2 = 1;
        Node head1 = node1, head2 = node2;
        // 相交
        while (node1.next != null) {
            len1++;
            node1 = node1.next;
        }
        while (node2.next != null) {
            len2++;
            node2 = node2.next;
        }
        if (node1 != node2) {
            return false;
        }

        int diff = 0;
        if (len1 >= len2) {
            diff = len1 - len2;
            while (diff > 0) {
                head1 = head1.next;
                diff--;
            }

        } else {
            diff = len2 - len1;
            while (diff > 0) {
                head2 = head2.next;
                diff--;
            }
        }

        while (head1 != null && head2 != null) {
            if (head1 == head2)
                return true;
            head1 = head1.next;
            head2 = head2.next;
        }
        return false;
    }

    /**
     * 相交给上第二种解法
     * 设链表A的长度为a，链表B的长度为b，A到相交结点的距离为c,B到相交节点的距离为d，
     * 显然可以得到两者相交链表的长度：a - c = b - d， 变换一下式子得到:a + d = b + c
     * 我们用一个指针从链表A出发，到末尾后就从B出发，用另一个指针从B出发，到末尾后从A出发，
     * 由于上面的公式，当前一个指针走了a+d步数时，后一个指针走了b+c,两步数相等，即走到了相交节点。
     * 相交：则t1 == t2退出循环
     * 不相交：t1和t2都到达链表末尾，都为null，相等退出
     */
    public Node getIntersectionNode(Node node1, Node node2) {
        Node t1 = node1;
        Node t2 = node2;
        while (t1 != t2) {
            if (t1 != null)
                t1 = t1.next;
            else
                t1 = node2;
            if (t2 != null)
                t2 = t2.next;
            else
                t2 = node1;
        }
        return t2;
    }
}
