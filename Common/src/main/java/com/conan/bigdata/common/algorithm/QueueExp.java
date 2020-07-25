package com.conan.bigdata.common.algorithm;

/**
 * 队列
 * FIFO(First In First Out): 先进先出，保持进出顺序一致
 * 可以通过数组和链表实现
 * <p>
 * 栈的使用不太多，队列在jdk中使用很广泛
 */
public class QueueExp<E> {

    // 定义一个链表的节点，单链表的实现，需要一个head节点，用来出队列，一个tail节点用来入队列，新加入的数据放在链表 尾
    private class Node<E> {
        private E data;
        public Node<E> next;

        public Node(E data) {
            this.data = data;
        }
    }

    private Node<E> head = null;
    private Node<E> tail = null;

    // 头节点为空，则表示没数据，队列为空
    public boolean isEmpty() {
        return head == null;
    }

    // 入队列
    public void add(E data) {
        Node<E> newNode = new Node<>(data);
        if (this.isEmpty()) {
            head = newNode;
            tail = newNode;
        }
        tail.next = newNode;
        tail = newNode;
    }

    // 出队列
    public E poll() {
        if (this.isEmpty()) {
            return null;
        }
        E e = head.data;
        head = head.next;
        return e;
    }

    public static void main(String[] args) {
        QueueExp<Integer> queue=new QueueExp<>();
        queue.add(1);
        queue.add(2);
        queue.add(3);

        System.out.println(queue.poll());
        System.out.println(queue.poll());
    }
}
