package com.conan.bigdata.common.designmode.decorator;

/**
 * 该package演示装饰器模式
 * 装饰器相较于继承来说，可以在运行时保持原来接口不变的情况下动态的给原对象增加新功能，而继承是静态的
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
