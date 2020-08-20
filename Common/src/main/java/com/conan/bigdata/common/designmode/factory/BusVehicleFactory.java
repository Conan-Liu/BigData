package com.conan.bigdata.common.designmode.factory;

public class BusVehicleFactory implements IVehicleFactory {

    @Override
    public IVehicle getVehicle() {
        return new BusVehicle();
    }

}
