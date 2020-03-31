package test;

import java.util.Map;
import java.util.TreeSet;

/**
 */
public class HelloWorld {
    public static void main(String[] args) {

        try {
            throw new Exception("aaaaa");
        } catch (Exception e) {
            e.printStackTrace();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                System.out.println("shutdown");
            }
        }));
    }
}