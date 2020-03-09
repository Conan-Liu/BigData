package com.conan.bigdata.common.pool.commonspool2;

public class Connection {

    private int id;

    public Connection(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return String.format("Connection id = {%d}", this.id);
    }
}