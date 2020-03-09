package com.conan.bigdata.common.algorithm;

import java.util.HashMap;
import java.util.Map;

/**
 * LRU算法简介
 * LRU（Least Recently Used）最近最久未使用算法
 * 常见应用场景：内存管理中的页面置换算法、缓存淘汰中的淘汰策略等
 * <p>
 * 实现理论
 * 　底层结构：双向链表 + HashMap ，双向链表由特定的哈希节点组成。
 * （1）访问节点时，将其从原来位置删除，插入到双向链表头部；
 * （2）更新节点时，先删除原有缓存数据（即原有节点），再将更新值作为节点插入链表头
 * （3）超过则执行淘汰；淘汰即删除双向链表最后一个节点，同时删除map中的映射
 * （4）LRU实现中有频繁的查找节点并删除，为节省时间（链表查找目标节点需要遍历），使用HashMap保存键-节点映射关系，O(1)的查找+O(1)的删除
 * （5）LRU实现中，要频繁的在头部插入，以及在尾部删除；因此，需要定义head、tail两个节点，方便操作
 *
 *
 * LinkedHashMap 也可以实现
 * 参考 {@link com.mysql.jdbc.util.LRUCache}
 * 重写removeEldestEntry方法即可
 */
public class LRU<K, V> {

    class Node {
        Node pre;
        Node next;
        V v;

        Node(V v) {
            this.v = v;
        }
    }

    // 保存头和尾，方便添加和删除
    private Node head;
    private Node tail;

    // map结构便于查找o(1)
    private Map<K, Node> map;
    // 定义缓存的容量
    private int maxSize;

    public LRU(int maxSize) {
        this.maxSize = maxSize;
        // 一次性申请完，避免动态扩展浪费资源
        this.map = new HashMap<>(maxSize * 4 / 3);
    }

    // 移除数据
    private void remove(K key) {
        Node node = map.get(key);

        node.next.pre = node.pre;
        node.pre.next = node.next;

        node.pre = null;
        node.next = null;
    }

    // 删除过期数据
    private void removeLast() {
        tail = tail.pre;
        tail.next = null;
    }

    // 添加或更新缓存数据
    public void put(K key, V value) {
        // 更新和添加数据都算最近使用，所以要放到链表最前方，更新 = 删除 + 添加
        // 删除
        if (map.containsKey(key)) {
            remove(key);
        }
        // 添加，先要判断map容量是否达到最大，决定是否先执行删除过期数据
        if (map.size() >= this.maxSize) {
            removeLast();
        }
        Node node = new Node(value);
        map.put(key, node);
        // 最近使用的数据放链表头部
        addFirst(node);
    }

    private void addFirst(Node node) {
        node.next = head.next;
        head.next.pre = node;

        head.next = node;
        node.pre = head;
    }

    public static void main(String[] args) {
    }
}