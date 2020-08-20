package com.conan.bigdata.common.designmode.factory;

public class BusVehicle implements IVehicle {

    @Override
    public void travel() {
        System.out.println("我坐公交车去游玩!!!");
    }

}