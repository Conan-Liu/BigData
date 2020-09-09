package leetcode.hot100;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LRU算法
 * 1. 可以使用{@link LinkedHashMap}，简单明了
 * 2. 哈希表 + 双向链表
 */
public class Code146 {

    // 第一种方法
    private class LRUCache extends LinkedHashMap<Integer, Integer> {
        // 记录容量
        private int capacity;

        public LRUCache(int capacity) {
            super(capacity, 0.75f, true);
            this.capacity = capacity;
        }

        public int get(int key) {
            return super.getOrDefault(key, -1);
        }

        public void put(int key, int value) {
            super.put(key, value);
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<Integer, Integer> eldest) {
            return super.size() > capacity;
        }
    }


    /**
     * 第二种方法
     * 可以自定义双向链表，也可以直接使用{@link }
     * 链表头表示最近访问的，链表尾用于删除淘汰或超过长度的数据，O(1)的删除插入
     * HashMap用于映射每个链表元素，实现O(1)查找
     */
    private class DeLinkNode {
        int key;
        int value;
        DeLinkNode pre;
        DeLinkNode next;

        DeLinkNode() {

        }

        DeLinkNode(int key, int value) {
            this.key = key;
            this.value = value;
        }

    }

    private Map<Integer, DeLinkNode> cache;
    private int size;
    private int capacity;
    private DeLinkNode head, tail;

    public Code146(int capacity) {
        this.capacity = capacity;
        this.size = 0;
        this.head = new DeLinkNode();
        this.tail = new DeLinkNode();
        this.head.next = tail;
        this.tail.pre = head;
        this.cache = new HashMap<>((int) (capacity / 0.75));
    }

    public int get(int key) {
        DeLinkNode deLinkNode = cache.get(key);
        if (deLinkNode == null) {
            return -1;
        }
        moveToHead(deLinkNode);
        return deLinkNode.value;
    }

    public void put(int key, int value) {
        DeLinkNode node = cache.get(key);
        if (node == null) {
            DeLinkNode newNode = new DeLinkNode(key, value);
            cache.put(key, newNode);
            addTodHead(newNode);
            size++;
            if (size > capacity) {
                DeLinkNode tail = removeTail();
                cache.remove(tail.key);
                size--;
            }
        } else {
            node.value = value;
            moveToHead(node);
        }
    }

    private void addTodHead(DeLinkNode node) {
        node.pre = head;
        node.next = head.next;
        head.next.pre = node;
        head.next = node;
    }

    private void removeNode(DeLinkNode node) {
        node.pre.next = node.next;
        node.next.pre = node.pre;
    }

    private void moveToHead(DeLinkNode node) {
        removeNode(node);
        addTodHead(node);
    }

    private DeLinkNode removeTail() {
        DeLinkNode res = tail.pre;
        removeNode(res);
        return res;
    }

    public static void main(String[] args) {
        Code146 cache=new Code146(2);
        cache.put(1,1);
        cache.put(2,2);
        System.out.println(cache.get(1));
        cache.put(3,3);
        System.out.println(cache.get(2));
        cache.put(4,4);
        System.out.println(cache.get(1));
        System.out.println(cache.get(3));
        System.out.println(cache.get(4));
    }
}
