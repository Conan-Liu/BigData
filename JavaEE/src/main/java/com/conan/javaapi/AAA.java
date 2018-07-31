package com.conan.javaapi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/5/17.
 */
public class AAA {
    public static void haha() {
        System.out.println("aaaa");
    }

    public static void main(String[] args) {
        List<String> list = new ArrayList<String>(10);
        try {
            throw new Exception("aaa");
        } catch (IOException e) {
            System.out.println("IO:" + e.getMessage());
        } catch (Exception e) {
            System.out.println("Ex:" + e.getMessage());
        }

        String s;

        System.out.println("5" + 2);
    }
}
