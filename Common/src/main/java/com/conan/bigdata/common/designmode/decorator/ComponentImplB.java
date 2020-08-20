package com.conan.bigdata.common.designmode.decorator;

/**
 */
public class ComponentImplB implements Component {

    private String name;

    public ComponentImplB(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }
}
