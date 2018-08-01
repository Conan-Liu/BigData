package com.conan.bigdata.common.factory;

public class CarVehicleFactory implements IVehicleFactory {

	public IVehicle getVehicle() {
		return new CarVehicle();
	}

}
