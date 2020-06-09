package com.conan.bigdata.common.concurrent.loadbalance;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * 定义的候选人节点，该类主要是提供服务的节点，抽象出来的概念
 */
// implements Iterable<Endpoint> 该代码是为了实现for语句对Endpoint的迭代遍历
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

    public int getTotalWeight() {
        return this.totalWeight;
    }

    public int getEndpointCount() {
        return this.endpointSet.size();
    }

    // 这里只为了对 Candidate 迭代出 Endpoint 对象
    @Override
    public Iterator<Endpoint> iterator() {
        final Iterator<Endpoint> iterator1 = endpointSet.iterator();
        return new Iterator<Endpoint>() {
            @Override
            public boolean hasNext() {
                return iterator1.hasNext();
            }

            @Override
            public Endpoint next() {
                return iterator1.next();
            }

            @Override
            public void remove() {

            }
        };
    }

    // 可以直接使用Set对象来迭代
    public Set<Endpoint> getEndpointSet() {
        return this.endpointSet;
    }

    @Override
    public String toString() {
        return "Candidate{" +
                "endpointSet=" + this.endpointSet +
                ", totalWeight=" + this.totalWeight +
                '}';
    }


    public static void main(String[] args) {
        Set<Endpoint> set = new HashSet<>();
        set.add(new Endpoint("h-1", 1));
        set.add(new Endpoint("h-2", 2));
        set.add(new Endpoint("h-3", 3));
        set.add(new Endpoint("h-4", 4));
        Candidate candidate = new Candidate(set);
        for (Endpoint endpoint : candidate) {
            System.out.println(endpoint);
        }
        System.out.println("*************************");
        for (Endpoint endpoint : candidate.getEndpointSet()) {
            System.out.println(endpoint);
        }
    }
}
