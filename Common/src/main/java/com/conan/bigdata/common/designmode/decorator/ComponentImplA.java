package com.conan.bigdata.common.designmode.decorator;

/**
 *
 */
public class ComponentImplA implements Component {

    private String name;

    public ComponentImplA(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }
}
