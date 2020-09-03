package com.conan.bigdata.common.javaapi;

import lombok.Data;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * java可以通过优先队列定义堆,默认是小根堆
 * 该队列是可以保存越来越多的数据，如果要使用小根堆来保存TopN的数据，可以使用queue.size()>N，不停的add，不停的poll
 */
public class PriorityQueueExp {

    /**
     * 优先级队列简单演示堆
     */
    private static void heap1() {
        PriorityQueue<Integer> queue = new PriorityQueue<>(8, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o2.compareTo(o1);
            }
        });

        Random random = new Random();
        random.ints(1, 100).limit(10).forEach(i -> {
            System.out.print(i + " ");
            queue.add(i);
            if (queue.size() >= 6) {
                queue.poll();
            }
        });

        System.out.println("\n堆内容输出");
        while (!queue.isEmpty()) {
            System.out.println(queue.poll());
        }
    }

    @Data
    private static class Order {
        String orderNo;
        int userId;
        long amt;

        Order(String orderNo, int userId, long amt) {
            this.orderNo = orderNo;
            this.userId = userId;
            this.amt = amt;
        }
    }

    private static void heap2() {
        PriorityQueue<Order> queue = new PriorityQueue<>(6, new Comparator<Order>() {
            @Override
            public int compare(Order o1, Order o2) {
                return o1.amt > o2.amt ? 1 : (o1.amt == o2.amt ? 0 : -1);
            }
        });

        Order o1 = new Order("101", 1, 10);
        Order o2 = new Order("102", 1, 10);
        Order o3 = new Order("103", 1, 9);
        Order o4 = new Order("104", 1, 8);
        Order o5 = new Order("105", 1, 11);
        Order o6 = new Order("106", 1, 14);

        queue.add(o1);
        queue.add(o2);
        queue.add(o3);
        queue.add(o4);
        queue.add(o5);
        queue.poll();
        queue.add(o6);
        queue.poll();

        System.out.println("堆内容输出");
        while (!queue.isEmpty()) {
            System.out.println(queue.poll());
        }
    }

    public static void main(String[] args) {
        heap2();
    }
}
