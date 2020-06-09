package com.conan.bigdata.common.concurrent.loadbalance;

import java.util.HashSet;
import java.util.Set;

/**
 * 该负载均衡功能的入口类，
 */
public class ServiceInvoker {

    // 唯一实例
    private static final ServiceInvoker INSTANCE = new ServiceInvoker();
    // 负载均衡可见性
    private volatile LoadBalancer loadBalancer;

    // 私有构造方法，不允许用户创建实例
    private ServiceInvoker() {

    }

    public LoadBalancer getLoadBalancer() {
        return this.loadBalancer;
    }

    private void setLoadBalancer(LoadBalancer loadBalancer) {
        this.loadBalancer = loadBalancer;
    }

    public static ServiceInvoker getInstance() {
        return INSTANCE;
    }

    // 根据LoadBalancer返回的Endpoint的节点来分发请求
    public void dispatchRequest(String msg) {
        Endpoint endpoint = getLoadBalancer().nextEndpoint();
        if (endpoint == null) {
            // 如果没返回，表示有异常，具体处理代码
            throw new IllegalArgumentException("未获取执行任务的资源节点Endpoint");
        }

        // 确定了Endpoint，然后接下来就是提交任务到具体的Endpoint上执行
        System.out.println(String.format("分发到节点[%s]，执行语句[%s]", endpoint, msg));
    }


    public static void main(String[] args) throws InterruptedException {
        Set<Endpoint> set = new HashSet<>();
        set.add(new Endpoint("h-1", 1));
        set.add(new Endpoint("h-2", 2));
        set.add(new Endpoint("h-3", 3));
        set.add(new Endpoint("h-4", 4));
        Candidate candidate = new Candidate(set);

        LoadBalancer balancer = HashLoadBalancer.getInstance(candidate);
        ServiceInvoker invoker = ServiceInvoker.getInstance();
        invoker.setLoadBalancer(balancer);

        for (int i = 100; i < 110; i++) {
            String s = "这是分发的内容-" + i;
            invoker.dispatchRequest(s);
        }
    }
}
