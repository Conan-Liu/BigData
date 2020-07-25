package com.conan.bigdata.common.algorithm;

/**
 * 栈
 * 栈的进出有限制，是一个限制级的线性表
 * LIFO(Last In First Out): 后进先出，可以使顺序逆序
 * 链表和数组都可以实现栈
 * 单链表可以很方便的实现栈，而且不用先申请空间，数组需要先申请空间，不利于扩展
 * Java.util.Stack类是基于Vector的，使用数组，可以自己实现Stack类，如下
 */
public class StackExp<E> {

    // 定义一个链表的节点，单链表的实现，需要固定一个head节点，用来出栈和进栈，新加入的数据放在链表 头
    private class Node<E> {
        private E data;
        public Node<E> next;

        public Node(E data) {
            this.data = data;
        }
    }

    private Node<E> head = null;

    // 头节点为空，则表示没数据，栈为空
    public boolean isEmpty() {
        return head == null;
    }

    // 入栈
    public void push(E data) {
        Node<E> newNode = new Node<>(data);
        newNode.next = head;
        head = newNode;
    }

    // 出栈
    public E pop() {
        if (this.isEmpty()) {
            return null;
        }
        E e = head.data;
        head = head.next;
        return e;
    }

    public static void main(String[] args) {
        StackExp<Integer> stack=new StackExp<>();
        stack.push(10);
        stack.push(20);
        stack.push(30);
        System.out.println(stack.pop());
        System.out.println(stack.pop());

    }


}
