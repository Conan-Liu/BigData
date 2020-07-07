package com.conan.bigdata.common.javaapi;

import java.util.ArrayList;
import java.util.List;

/**
 * 该代码需要Java8环境，否则语法不通过
 * 定义一个Java8的接口，可以有默认方法，非static
 *
 * 这里假设有很多类已经实现Human接口，一旦接口增加一个方法，对于该接口实现所有的类都要增加这个方法的实现，工作量太大
 * 如果使用Java8，可以在接口中添加默认方法，实现类可以不用增加该方法
 * 注意接口默认方法由default修饰，需要写方法体代码，有点像抽象类方法
 * 一个接口可以有多个默认方法
 */
interface Human {

    void say(String msg);

    default void walk(){
        System.out.println("every one can walk");
    }
}

class Man implements Human {

    @Override
    public void say(String msg) {
        System.out.println("man say : " + msg);
    }
}

class Woman implements Human {

    @Override
    public void say(String msg) {
        System.out.println("woman say : " + msg);
    }
}

public class Java8Interface {

    public static void main(String[] args) {
        Human h=new Woman();
        h.say("hello");
        h.walk();

        // List 接口默认的sort方法
        List<String> list=new ArrayList<>();

    }
}
