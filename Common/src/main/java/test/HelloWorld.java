package test;

import java.util.Map;
import java.util.TreeSet;

/**
 */
public class HelloWorld {
    public static void main(String[] args) {
        System.out.println("Hello World");

        TreeSet<Integer> a = new TreeSet<>();
        for (int i = 0; i < 100; i++) {
            a.add(i);
            if (a.size() > 10) {
                a.pollFirst();
            }
        }
        System.out.println(a);

        a.add(100);
        a.add(99);
        System.out.println(a);

    }
}