package com.conan.bigdata.common.pool.commonspool1;

/**
 * Created by Administrator on 2019/5/6.
 */
public class Connection {

    private static int id = 0;
    private int v;

    public Connection() {
        this.v = id++;
        System.out.println("create " + this.v);
    }

    public void destory() {
        System.out.println("destory " + this.v);
    }

    public int getV() {
        return this.v;
    }

    @Override
    public String toString() {
        return String.format("Connection id = {%d}", id);
    }
}