package com.conan.bigdata.common.concurrent.loadbalance;

import java.util.Random;

/**
 * 具体的负载均衡实现算法
 * hash 方法负载均衡
 */
public class HashLoadBalancer extends AbstractLoadBalancer {

    private static LoadBalancer balancer = null;

    public HashLoadBalancer(Candidate candidate) {
        super(candidate);
        LOG.info("HashLoadBalancer 初始化...");
        super.init();
    }

    public static LoadBalancer getInstance(Candidate candidate) {
        if (balancer == null) {
            balancer = new HashLoadBalancer(candidate);
        }

        return balancer;
    }

    // 具体实现
    @Override
    public Endpoint nextEndpoint() {
        Endpoint endpoint = null;

        Random random = new Random();
        int i = 0;
        int rand = Math.abs(random.nextInt() % super.candidate.getEndpointCount());
        for (Endpoint endpoint1 : super.candidate) {
            // LOG.info("随机数 i=" + i + ", rand=" + rand);
            if (i == rand) {
                endpoint = endpoint1;
                break;
            }
            i++;
        }
        return endpoint;
    }
}
