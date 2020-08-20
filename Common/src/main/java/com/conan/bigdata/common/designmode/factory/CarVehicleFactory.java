package com.conan.bigdata.common.designmode.factory;

public class CarVehicleFactory implements IVehicleFactory {

    @Override
    public IVehicle getVehicle() {
        return new CarVehicle();
    }

}
