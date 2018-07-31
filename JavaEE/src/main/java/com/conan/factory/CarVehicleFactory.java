package com.conan.factory;

public class CarVehicleFactory implements IVehicleFactory {

	public IVehicle getVehicle() {
		return new CarVehicle();
	}

}
