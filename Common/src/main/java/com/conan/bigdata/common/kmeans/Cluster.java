package com.conan.bigdata.common.kmeans;

import java.util.ArrayList;
import java.util.List;

public class Cluster {

	public int id;
	public Point center;
	public List<Point> members = new ArrayList<Point>();

	public Cluster(int id, Point center) {
		this.id = id;
		this.center = center;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Cluster)
			return this.id == ((Cluster) obj).id;
		else
			return false;
	}
}
