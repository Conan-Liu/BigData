package com.conan.factory;

public class TestFactory {

	/*
	 * 简单点说工厂模式就是: 你想要什么产品,就用对应的产品工厂去生产
	 * 产品可以抽象,比如: 可以乘车去玩
	 * 产品具体化, 比如: 可以选择乘不同的车去玩，私家车，公共车
	 * 工厂可以抽象,比如: 工厂都可以生成车
	 * 工厂具体化, 比如: 一个工厂只生产一种车
	 * 使用者可以去具体的工厂里面获取具体的产品，来使用，妥妥滴工厂模式
	 */
	public static void main(String[] args) {
		IVehicleFactory factory = null;

		factory = new BusVehicleFactory();
		IVehicle bus = factory.getVehicle();
		bus.travel();

		factory = new CarVehicleFactory();
		IVehicle car = factory.getVehicle();
		car.travel();
	}

}
