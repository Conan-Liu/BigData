package com.conan.bigdata.common.designmode.decorator;

/**
 * 具体的装饰器B
 */
public class ComponentDecoratorImplB extends ComponentDecorator {

    public ComponentDecoratorImplB(Component component) {
        super(component);
    }

    @Override
    public String getName() {
        return super.getName() + "|append2";
    }
}
