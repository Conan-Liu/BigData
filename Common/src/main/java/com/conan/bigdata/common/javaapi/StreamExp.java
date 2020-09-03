package com.conan.bigdata.common.javaapi;

import lombok.Builder;
import lombok.Data;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.*;

/**
 * Java8 Stream的操作
 * 注意Stream流对象经过Terminal操作后，因为Stream元素被消费掉，无法在继续使用了
 * 先stream.forEach，然后stream.map就会报错
 */
public class StreamExp {

    // 如何构造Stream流
    private static void create() {
        // 独立的元素
        Stream<String> stringStream = Stream.of("a", "b", "c");

        // 数组
        String[] strArray = new String[]{"a", "b", "c"};
        Stream<String> strArrayStream1 = Stream.of(strArray);
        Stream<String> strArrayStream2 = Arrays.stream(strArray);
        strArrayStream2.forEach(System.out::print);
        System.out.println();

        // 集合
        List<String> list = Arrays.asList(strArray);
        Stream<String> listStream = list.stream();

        // 数值流的构造，虽然可以使用Stream<T>的类型，但是jdk对三种基本数据类型提供了相应的Stream，减少装箱和拆箱的性能消耗
        IntStream intStream = IntStream.of(1, 3, 4, 5, 6, 6, 7);
        IntStream range = IntStream.range(1, 3);
        LongStream longStream = LongStream.of(1L, 2L, 4L);
        DoubleStream doubleStream = DoubleStream.of(1.0, 2.0, 3.0);
        doubleStream.forEach(System.out::print);
    }


    // 流转换为其它数据结构
    private static void transform() {
        Stream<String> stringStream = Stream.of("a", "b", "c");

        List<String> collect = stringStream.collect(Collectors.toList());
    }


    // 常规操作
    private static void api() {
        // 转换大写
        Stream<String> stringStream = Stream.of("abXc", "hahah", "YYsd66");
        List<String> list1 = stringStream.map(String::toUpperCase).collect(Collectors.toList());
        list1.forEach(System.out::println);

        // 平方数
        IntStream intStream = IntStream.of(1, 2, 3, 4);
        intStream.map(x -> x * x).forEach(System.out::print);

        // 一对多flatMap
        Stream<List<Integer>> listStream = Stream.of(Arrays.asList(1), Arrays.asList(1, 2), Arrays.asList(1, 2, 3));
        listStream.flatMap(x -> x.stream()).forEach(System.out::println);

        // filter
        String line = "hello,world, ,hadoop,";
        Stream<String> split = Stream.of(line.split(","));
        split.filter(w -> w.trim().length() > 0).forEach(System.out::println);

        // limit skip
        LongStream longStream = LongStream.range(1, 100000000000L);
        LongStream ls = longStream.limit(10);
        ls.forEach(System.out::print);
        System.out.println();

        // 打印
        System.out.println("java8...");
        String[] ss = "hadoop,spark, ,hive,hbase,flink".split(",");
        Stream.of(ss).filter(w -> {
            // 流式数据一条一条的处理
            System.out.println(w);
            return w.trim().length() > 0;
        }).forEach(p -> System.out.println(p));
        // java8的方法等同于
        System.out.println("pre-java8...");
        for (int i = 0; i < ss.length; i++) {
            if (ss[i].trim().length() > 0) {
                System.out.println(ss[i]);
            }
        }
    }

    // reduce 的用例单独演示
    private static void reduceExp() {
        // 字符串连接
        String s = Stream.of("A", "B", "c", "d").reduce("reduce => ", String::concat);
        System.out.println(s);

        double minValue = Stream.of(-1.5, 1.0, -3.0, -2.0).reduce(Double::min).get();
        double minValue1 = Stream.of(-1.5, 1.0, -3.0, -2.0).reduce(Math::min).get();

        int sum = Stream.of(1, 2, 3, 4).reduce(Integer::sum).get();
        int sum1 = Stream.of(1, 2, 3, 4).reduce(0, (x, y) -> x + y);

        double avg = Stream.of(1, 2, 3, 4).collect(Collectors.averagingInt(p -> p));
        double asDouble = Stream.of(1, 2, 3, 4).mapToInt(x -> x).average().getAsDouble();
        System.out.println(avg + " " + asDouble);
        System.out.println(minValue + " " + sum);
    }

    // IO流演示
    private static void br() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("/Users/mw/temp/w"));
        Stream<String> lines = br.lines();
        int asInt = lines.mapToInt(String::length).max().getAsInt();
        br.close();
        System.out.println(asInt);
    }

    // 特殊的数据流
    private static void special() {
        Random random = new Random();
        IntStream ints = random.ints(1, 100);
        ints.limit(10).sorted().forEach(System.out::println);
    }

    public static void main(String[] args) throws Exception {
        // create();
//         api();
//        reduceExp();
//        br();
        // special();
//        fullExp();
        show();
    }

    private static void fullExp() {
        List<String> strings = Arrays.asList("abc", "", "bc", "efg", "abcd", "", "jkl");

        long count = strings.stream().filter(x -> x.isEmpty()).count();
        System.out.println("空字符串数量: " + count);

        count = strings.stream().filter(x -> x.length() == 3).count();
        System.out.println("字符串长度为3的数量: " + count);

        List<String> filter = strings.stream().filter(x -> !x.isEmpty()).collect(Collectors.toList());
        System.out.println("筛选后的列表: " + filter);

        String collect = strings.stream().collect(Collectors.joining(","));
        System.out.println("合并字符串: " + collect);

        System.out.println("flatMap: ");
        strings.stream().flatMap(x -> {
            char[] chars = x.toCharArray();
            List<Character> list = new ArrayList<>();
            for (char aChar : chars) {
                list.add(aChar);
            }
            return list.stream();
        }).forEach(System.out::print);


        List<Integer> integers = Arrays.asList(1, 2, 13, 4, 15, 6, 17, 8, 19);
        IntSummaryStatistics intSummaryStatistics = integers.stream().mapToInt(x -> x).summaryStatistics();
        System.out.println("列表最大数: " + intSummaryStatistics.getMax());
        System.out.println("列表最小数: " + intSummaryStatistics.getMin());
        System.out.println("所有数之和: " + intSummaryStatistics.getSum());
        System.out.println("平均数: " + intSummaryStatistics.getAverage());

        int sum = integers.stream().mapToInt(x -> x).sum();
        System.out.println();

        // Match匹配操作
        boolean b = integers.stream().allMatch(x -> x > 10);
        System.out.println("是否全部大于10: " + b);

        boolean c = integers.stream().anyMatch(x -> x > 10);
        System.out.println("是否部分大于10: " + c);

        boolean d = integers.stream().noneMatch(x -> x < 2);
        System.out.println("是否全部不匹配: " + d);
    }

    /**
     * Stream来计算两个List集合的关联
     */
    private static void show() {
        List<User> userList = new ArrayList<>();
        userList.add(new User(1, "zhangsan"));
        userList.add(new User(2, "lisi"));
        userList.add(new User(3, "wanger"));
        List<Order> orderList = new ArrayList<>();
        orderList.add(new Order("100", 1, 12.3));
        orderList.add(new Order("101", 2, 13.4));
        orderList.add(new Order("102", 1, 9.9));
        orderList.add(new Order("103", 1, 90.9));

        Stream<OrderVO> orderVOStream = orderList.stream().map(
                o -> userList.stream().filter(u -> o.userId == u.userId)
                        .findFirst().map(
                                x -> OrderVO.builder().orderNo(o.getOrderNo()).name(x.getName()).amt(o.getAmt()).build()
                        ).orElse(new OrderVO(o.getOrderNo(), "", o.getAmt()))
        );

        Map<String, Double> collect = orderVOStream.collect(Collectors.groupingBy(OrderVO::getName, Collectors.summingDouble(OrderVO::getAmt)));

        Stream<Map.Entry<String, Double>> sorted = collect.entrySet().stream().sorted(Map.Entry.<String, Double>comparingByValue().reversed());

        Map.Entry<String, Double> stringDoubleEntry = sorted.findFirst().get();
        System.out.println(stringDoubleEntry.getKey() + " " + stringDoubleEntry.getValue());
    }

    @Data
    @Builder
    private static class OrderVO {
        String orderNo;
        String name;
        double amt;

        OrderVO(String orderNo, String name, double amt) {
            this.orderNo = orderNo;
            this.name = name;
            this.amt = amt;
        }
    }

    @Data
    private static class User {
        int userId;
        String name;

        User(int userId, String name) {
            this.userId = userId;
            this.name = name;
        }
    }

    @Data
    private static class Order {
        String orderNo;
        int userId;
        double amt;

        Order(String orderNo, int userId, double amt) {
            this.orderNo = orderNo;
            this.userId = userId;
            this.amt = amt;
        }
    }
}
