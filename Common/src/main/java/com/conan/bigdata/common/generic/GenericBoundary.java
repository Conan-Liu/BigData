package com.conan.bigdata.common.generic;

/**
 * Created by Administrator on 2019/4/9.
 */
// 在使用泛型的时候，我们还可以为传入的泛型类型实参进行上下边界的限制，如：类型实参只准传入某种类型的父类或某种类型的子类。
class Generic1<T> {
    private T key;

    public Generic1(T t) {
        this.key = t;
    }

    public T getKey() {
        return this.key;
    }
}

class Generic2<T extends Number> {
    private T key;

    public Generic2(T t) {
        this.key = t;
    }

    public T getKey() {
        return this.key;
    }
}

public class GenericBoundary {
    public static void showKey1(Generic1<? extends Number> obj) {
        System.out.println("泛型测试: " + obj.getKey());
    }

    public static void main(String[] args) {
        Generic1<String> stringGeneric1 = new Generic1<>("abc");
        Generic1<Integer> integerGeneric1 = new Generic1<>(123);
        Generic1<Double> doubleGeneric1 = new Generic1<>(456.78);

//        GenericBoundary.showKey1(stringGeneric);  //编译出错
        GenericBoundary.showKey1(integerGeneric1);
        GenericBoundary.showKey1(doubleGeneric1);


//        Generic2<String> stringGeneric2 = new Generic2<>("abc");  // 报错， 这个类的泛型参数类型需要继承至Number类
        Generic2<Integer> integerGeneric2 = new Generic2<>(123);
        Generic2<Double> doubleGeneric2 = new Generic2<>(456.78);
    }
}