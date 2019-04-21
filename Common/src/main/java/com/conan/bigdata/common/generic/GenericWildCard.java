package com.conan.bigdata.common.generic;

/**
 * Created by Administrator on 2019/4/9.
 */
// 泛型通配符
public class GenericWildCard {
    public static void showKeyValue1(GenericClass<Number> obj) {
        System.out.println("泛型测试: " + obj.getKey());
    }

    public static void showKeyValue2(GenericClass<?> obj) {
        System.out.println("泛型测试: " + obj.getKey());
    }

    public static void main(String[] args) {
        GenericClass<Integer> gInteger = new GenericClass<>(123);
        GenericClass<Integer> gNumber = new GenericClass<>(456);

//        GenericWildCard.showKeyValue1(gInteger); // 这个方法编译不通过，类型不匹配
        GenericWildCard.showKeyValue2(gInteger);
        GenericWildCard.showKeyValue2(gNumber);
    }
}