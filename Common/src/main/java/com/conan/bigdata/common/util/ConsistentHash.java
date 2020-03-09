package com.conan.bigdata.common.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * 实现一致性Hash算法中使用的哈希函数，使用MD5算法保证一致性哈希的平衡性
 * 这里可以参考网上一些其它的方法
 */
public class ConsistentHash<T> {

    // 一致性hash算法
    private static class HashFunction {
        private MessageDigest md5 = null;

        public long hash(String key) {
            if (md5 == null) {
                try {
                    md5 = MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException e) {
                    throw new IllegalStateException("no md5 algrithm found");
                }
            }

            md5.reset();
            md5.update(key.getBytes());

            byte[] md5Key = md5.digest();
            long result = ((long) (md5Key[3] & 0xFF) << 24) | ((long) (md5Key[2] & 0xFF) << 16) | ((long) (md5Key[1] & 0xFF) << 8) | ((long) (md5Key[0] & 0xFF));

            return result & 0xFFFFFFFFL;
        }
    }

    private final HashFunction hashFunction;
    // 服务器节点的复制因子， 虚拟节点个数 = 实际节点个数 * numOfReplicas
    private final int numOfReplicas;
    private final SortedMap<Long, T> circle = new TreeMap<>();

    // node 是实际的节点
    public ConsistentHash(HashFunction hashFunction, int numOfReplicas, Collection<T> nodes) {
        this.hashFunction = hashFunction;
        this.numOfReplicas = numOfReplicas;
        for (T node : nodes) {
            add(node);
        }
    }

    // 添加指定 numOfReplicas 数目的虚拟节点
    public void add(T node) {
        for (int i = 0; i < numOfReplicas; i++) {
            circle.put(hashFunction.hash(node.toString() + i), node);
        }
    }

    // 移除实际节点
    public void remove(T node) {
        for (int i = 0; i < numOfReplicas; i++) {
            circle.remove(hashFunction.hash(node.toString() + i));
        }
    }

    /*
    * 获得一个最近的顺时针节点,根据给定的key 取Hash
    * 然后再取得顺时针方向上最近的一个虚拟节点对应的实际节点
    * 再从实际节点中取得 数据
    */
    public T get(String key) {
        if (circle.isEmpty()) {
            return null;
        }

        long hashKey = hashFunction.hash(key);
        if (!circle.containsKey(hashKey)) {
            // 这里返回 K 大于等于 hashKey 的所有值， 依旧是SortedMap
            SortedMap<Long, T> tailMap = circle.tailMap(hashKey);
            hashKey = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }

        return circle.get(hashKey);
    }

    public long getSize() {
        return circle.size();
    }

    public void testBalance() {
        Set<Long> sets = circle.keySet();
        SortedSet<Long> sortedSets = new TreeSet<>(sets);
        for (Long hashKey : sortedSets) {
            System.out.println(hashKey);
        }

        System.out.println("----each location's distance are follows: ----");
        /*
         * 查看用MD5算法生成的long hashCode相邻两个hashCode的差值
         */
        Iterator<Long> it = sortedSets.iterator();
        Iterator<Long> it2 = sortedSets.iterator();
        if (it2.hasNext())
            it2.next();

        long keyPre, keyAfter;
        while (it.hasNext() && it2.hasNext()) {
            keyPre = it.next();
            keyAfter = it2.next();
            System.out.println(keyAfter - keyPre);
        }
    }

    public static void main(String[] args) {
        // 这里如果是服务器， 可以使用 ip 地址
        Set<String> nodes = new HashSet<>();
        nodes.add("A");
        nodes.add("B");
        nodes.add("C");
        nodes.add("D");

        ConsistentHash<String> consistentHash = new ConsistentHash<>(new HashFunction(), 3, nodes);
        consistentHash.add("E");

        System.out.println("hash circle size: " + consistentHash.getSize());
        System.out.println("===============");
        consistentHash.testBalance();
    }
}