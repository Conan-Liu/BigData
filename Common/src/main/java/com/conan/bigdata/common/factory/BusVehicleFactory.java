package com.conan.bigdata.common.factory;

public class BusVehicleFactory implements IVehicleFactory {

    @Override
    public IVehicle getVehicle() {
        return new BusVehicle();
    }

}
