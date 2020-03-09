package com.conan.bigdata.common.factory;

public class CarVehicle implements IVehicle {

    @Override
    public void travel() {
        System.out.println("自己开车去游玩!!!");
    }

}
