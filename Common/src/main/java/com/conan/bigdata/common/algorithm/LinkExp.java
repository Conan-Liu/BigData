package com.conan.bigdata.common.algorithm;

import lombok.Data;


/**
 * 链表的相关操作
 * 单链表和双链表
 * 双链表相对而言简单点
 * 以下代码以单链表为主，链表可以参考LinkedList，内部维护一个size，避免遍历时需要先计算长度
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
    private static class Node1<E>{
        private E data;
        public Node1<E> next;

        public Node1(E data){
            this.data=data;
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
     */
    private static void isLoop(Node node) {
        // node作为第一个指针，p1作为第二个
        Node p1 = node;
        while (node != null && node.next != null) {
            node = node.next.next;
            p1 = p1.next;
            if (node == p1) {
                System.out.println("isLoop");
                break;
            }
        }
        if (node == null || node.next == null)
            System.out.println("noLoop");
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
     * 判断两个链表是否相交
     * 两个链表如果有相同的尾节点，那他们是必然相交的
     *
     * 在确定相交的同时记录下每个链表的长度，可以计算两个链表的长度差diff，这样长的那个链表先遍历diff后，两个链表同步遍历，遇到两个引用相同时，该节点就是相交点
     */
    private static void isIntersect(Node node1, Node node2) {
        while (node1.next != null) {
            node1 = node1.next;
        }
        while (node2.next != null) {
            node2 = node2.next;
        }
        if (node1 == node2) {
            System.out.println("true");
        } else {
            System.out.println("false");
        }
    }
}
