package com.conan.bigdata.common.designmode.decorator;

/**
 * 装饰器实现component接口
 */
public abstract class ComponentDecorator implements Component {

    // 定义指向Component的实例引用
    protected Component component;

    public ComponentDecorator(Component component) {
        this.component = component;
    }

    // Decorator继承ComponentImpl实际业务1，可添加的基类的装饰业务
    @Override
    public String getName() {
        return component.getName();
    }
}
