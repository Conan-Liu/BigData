package test;

import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

public class AAA {

    private static class T1 implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    System.out.println("t1-" + UUID.randomUUID().toString());
                    Thread.sleep(2000);
                    System.exit(0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static class T2 implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    System.out.println("t2-" + UUID.randomUUID().toString());
                    Thread.sleep(20000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("aaaa");
            }
        }
    }

    public static void main(String[] args) throws IOException {

        Set<Integer> set=new HashSet<>();
    }

}
