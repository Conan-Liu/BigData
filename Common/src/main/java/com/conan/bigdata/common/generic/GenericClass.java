package com.conan.bigdata.common.generic;

// 泛型类
// 如果有多个泛型类型的字段， 写法如下
// GenericClass<T1,T2>
// 以此类推
public class GenericClass<T> {

    private T key;

    public GenericClass(T key) {
        this.key = key;
    }

    public void setKey(T key) {
        this.key = key;
    }

    public T getKey() {
        return key;
    }

    public static void main(String[] args) {
        GenericClass<Integer> genericTest1 = new GenericClass<>(1);
        GenericClass<String> genericTest2 = new GenericClass<>("a");

        genericTest1.setKey(123);
        genericTest2.setKey("abc");
        System.out.println("泛型测试1: " + genericTest1.getKey());
        System.out.println("泛型测试2: " + genericTest2.getKey());

    }
}