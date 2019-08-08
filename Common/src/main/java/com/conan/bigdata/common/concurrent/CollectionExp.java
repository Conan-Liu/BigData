package com.conan.bigdata.common.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Administrator on 2019/8/8.
 */
public class CollectionExp {

    public static void main(String[] args) {
        List<String> list = Collections.synchronizedList(new ArrayList<>());
        list.add("aaaa");
    }

}