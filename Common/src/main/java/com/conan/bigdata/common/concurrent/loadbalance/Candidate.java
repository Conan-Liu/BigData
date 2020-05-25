package com.conan.bigdata.common.concurrent.loadbalance;

import java.util.Iterator;
import java.util.Set;

/**
 * 定义的候选人节点，该类主要是提供服务的节点，抽象出来的概念
 */
public class Candidate implements Iterable<Endpoint> {

    private final Set<Endpoint> endpointSet;
    private final int totalWeight;

    public Candidate(Set<Endpoint> endpoints) {
        int sum = 0;
        for (Endpoint e : endpoints) {
            sum += e.getWeight();
        }
        this.totalWeight = sum;
        this.endpointSet = endpoints;
    }

    @Override
    public Iterator<Endpoint> iterator() {
        return null;
    }
}
