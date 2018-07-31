package com.conan.factory;

public class BusVehicleFactory implements IVehicleFactory {

	public IVehicle getVehicle() {
		return new BusVehicle();
	}

}
