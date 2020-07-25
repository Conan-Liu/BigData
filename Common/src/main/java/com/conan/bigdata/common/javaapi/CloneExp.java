package com.conan.bigdata.common.javaapi;

/**
 * java 对象传参数和赋值都是引用，不会改变对象的值，相当于多创建一个快捷方式
 * 可以通过Cloneable接口来实现对象的赋值，相当于文件的复制
 *
 * 如果对象中包含其它的成员对象
 * 浅复制：对成员对象的引用复制，还是指向原来的对象
 * 深复制：对成员对象继续调用该成员对象的clone方法，再复制一份新对象
 */
public class CloneExp implements Cloneable{

    private int i=0;

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public static void main(String[] args) throws CloneNotSupportedException {
        CloneExp cloneExp = new CloneExp();
        CloneExp b=cloneExp;
        CloneExp c=(CloneExp)cloneExp.clone();
        cloneExp.i=100;
        System.out.println(b.i);
        System.out.println(cloneExp.i);
        System.out.println(c.i);
    }
}
