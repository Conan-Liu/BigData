package com.conan.bigdata.common.concurrent.loadbalance;

/**
 * 负载均衡接口类，定义标准，方便使用各种负载均衡算法
 * 负载均衡主要有两个功能
 * 1. 监控可以提供服务的节点作为候选人
 * 2. 返回被分配的节点
 */
public interface LoadBalancer {

    // 监控候选人
    void updateCandidate(Candidate candidate);

    // 返回分配的节点
    Endpoint nextEndpoint();
}
