package com.conan.bigdata.common.factory;

public class BusVehicleFactory implements IVehicleFactory {

	public IVehicle getVehicle() {
		return new BusVehicle();
	}

}
