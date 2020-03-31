package com.conan.bigdata.common.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CollectionExp {

    public static void main(String[] args) {
        List<String> list = Collections.synchronizedList(new ArrayList<String>());
        list.add("aaaa");


        ConcurrentHashMap<Integer, String> map = new ConcurrentHashMap<>(20);
        map.put(1, "aaa");
        map.put(2, "bbb");
        for (Map.Entry<Integer, String> v : map.entrySet()) {
            System.out.println(v.getKey() + " - " + v.getValue());
        }

    }

}