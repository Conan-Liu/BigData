package com.conan.bigdata.common.generic;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestA {

    @Test
    public void test1() {
        List<String> stringArrayList = new ArrayList<>();
        List<Integer> integerArrayList = new ArrayList<>();

        Class classStringArrayList = stringArrayList.getClass();
        Class classIntegerArrayList = integerArrayList.getClass();

        if (classStringArrayList.equals(classIntegerArrayList)) {
            System.out.println(classIntegerArrayList.getName());
            System.out.println("类型相同");
        }
    }
}