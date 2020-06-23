package test;

import com.conan.bigdata.common.javaapi.AnnotationExp;

import java.util.Map;
import java.util.TreeSet;

/**
 */

public class HelloWorld {

    @AnnotationExp(name = "ahahaha",age = {1,2,3})
    private String name;

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