package com.conan.bigdata.common.designmode.decorator;

/**
 * 具体的装饰器A
 */
public class ComponentDecoratorImplA extends ComponentDecorator {

    public ComponentDecoratorImplA(Component component) {
        super(component);
    }

    // 增加额外操作
    public void appendName(String name) {
        System.out.println(component.getName() + name);
    }

}
