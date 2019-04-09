package com.conan.bigdata.common.generic;

/**
 * Created by Administrator on 2019/4/9.
 */
// 泛型方法
public class GenericMethod {

    // 普通泛型方法
    public static <T> T showKeyName(GenericClass<T> gec) {
        T key = gec.getKey();
        return key;
    }

    // 这也是泛型方法， 可以不与泛型T同一个类型
    public static <E> E show2(E t) {
        return t;
    }

    // 泛型可变长参数
    public static <E> void printMsg(E... args) {
        for (E e : args) {
            System.out.println(e);
        }
    }

    public static void main(String[] args) {
        GenericClass<Integer> gec = new GenericClass<>(123);
        System.out.println(GenericMethod.showKeyName(gec));
        GenericMethod.printMsg("a", "b", "c", 123, 55.5);
    }
}