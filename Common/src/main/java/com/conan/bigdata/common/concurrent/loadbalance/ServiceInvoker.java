package com.conan.bigdata.common.concurrent.loadbalance;

/**
 * 该负载均衡功能的入口类，
 */
public class ServiceInvoker {

    private static final ServiceInvoker INSTANCE = new ServiceInvoker();
    // 负载均衡可见性
    private volatile LoadBalancer loadBalancer;

    private ServiceInvoker() {
        this.loadBalancer = new HashLoadBalancer(null);
    }

    private void setLoadBalancer(LoadBalancer loadBalancer) {
        this.loadBalancer = loadBalancer;
    }

    public static ServiceInvoker getInstance() {
        return INSTANCE;
    }

    // 根据LoadBalancer返回的Endpoint的节点来分发请求
    public void dispatchRequest(String msg) {
        Endpoint endpoint = this.loadBalancer.nextEndpoint();
        if (endpoint == null) {
            // 如果没返回，表示有异常，具体处理代码
        }

        // 确定了Endpoint，然后接下来就是提交任务到具体的Endpoint上执行
        System.out.println(String.format("分发到节点[%s]，执行语句[%s]", endpoint, msg));
    }

    public static void main(String[] args) {
        ServiceInvoker invoker = ServiceInvoker.getInstance();
        invoker.dispatchRequest("这是分发的内容");
    }
}
