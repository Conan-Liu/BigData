package com.conan.bigdata.common.concurrent.loadbalance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * 抽象类，实现负载均衡的一些共有功能
 */
public abstract class AbstractLoadBalancer implements LoadBalancer {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractLoadBalancer.class);

    private volatile Candidate candidate;
    private final Random random;
    // 定义一个心跳线程来监控节点是否有效
    private Thread heartbeatThread;

    public AbstractLoadBalancer(Candidate candidate) {
        if (candidate == null) {
            throw new IllegalArgumentException("Invalid candidate: " + candidate);
        }
        this.candidate = candidate;
        this.random = new Random();
    }

    // 启动心跳线程监控候选人
    public synchronized void init() {
        if (this.heartbeatThread == null) {
            // 成员方法可以直接调用内部类
            // 静态方法调用静态内部类
            this.heartbeatThread = new Thread(new HeartbeatTask(), "heartbeat-task");
            this.heartbeatThread.setDaemon(true);
            this.heartbeatThread.start();
        }
    }

    @Override
    public void updateCandidate(Candidate candidate) {
        this.candidate = candidate;
    }

    // 该获取节点的抽象方法由具体的负载均衡算法来实现，个性化
    @Override
    public abstract Endpoint nextEndpoint();


    // 负载均衡提供一个共有的有效节点心跳检测
    private class HeartbeatTask implements Runnable {

        @Override
        public void run() {
            try {
                while (true) {
                    // 检测节点是否有效
                    monitorEndpoint();

                    // 每三秒心跳一次
                    Thread.sleep(3000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void monitorEndpoint() {
            // 检测节点代码
            boolean isEndpointOnline;
            for (Endpoint endpoint : candidate) {
                isEndpointOnline = endpoint.isOnline();
                if (doDetect(endpoint) != isEndpointOnline) {
                    endpoint.setOnline(!isEndpointOnline);
                }
            }
        }

        private boolean doDetect(Endpoint endpoint) {
            return true;
        }
    }
}
