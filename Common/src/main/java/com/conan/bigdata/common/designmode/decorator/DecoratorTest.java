package com.conan.bigdata.common.designmode.decorator;

/**
 * 该package演示装饰器模式
 * 启动类
 */
public class DecoratorTest {

    public static void main(String[] args) {
        Component componentA=new ComponentImplA("componentA");

        ComponentDecoratorImplA decoratorImplA=new ComponentDecoratorImplA(componentA);
        decoratorImplA.appendName("|append1");

        ComponentDecoratorImplB decoratorImplB=new ComponentDecoratorImplB(componentA);
        System.out.println(decoratorImplB.getName());
    }
}
