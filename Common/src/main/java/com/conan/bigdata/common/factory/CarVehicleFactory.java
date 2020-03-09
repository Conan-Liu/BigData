package com.conan.bigdata.common.factory;

public class CarVehicleFactory implements IVehicleFactory {

    @Override
    public IVehicle getVehicle() {
        return new CarVehicle();
    }

}
