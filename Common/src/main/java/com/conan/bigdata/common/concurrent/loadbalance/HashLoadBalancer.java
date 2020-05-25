package com.conan.bigdata.common.concurrent.loadbalance;

/**
 * 具体的负载均衡实现算法
 * hash 方法负载均衡
 */
public class HashLoadBalancer extends AbstractLoadBalancer{

    public HashLoadBalancer(Candidate candidate) {
        super(candidate);
    }

    // 具体实现
    @Override
    public Endpoint nextEndpoint() {
        return null;
    }
}
