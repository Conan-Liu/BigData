package com.conan.bigdata.common.concurrent.loadbalance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * 抽象类，实现负载均衡的一些共有功能
 */
public abstract class AbstractLoadBalancer implements LoadBalancer {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractLoadBalancer.class);

    protected volatile Candidate candidate;
    private final Random random;
    // 定义一个心跳线程来监控节点是否有效
    private Thread heartbeatThread;

    public AbstractLoadBalancer(Candidate candidate) {
        // 没有候选人，则报异常
        if (candidate == null || candidate.getEndpointCount() == 0) {
            throw new IllegalArgumentException("Invalid candidate: " + candidate);
        }
        this.candidate = candidate;
        this.random = new Random();
    }

    // 启动心跳线程监控候选人，不在构造方法中启动线程，防止逸出
    public synchronized void init() {
        if (this.heartbeatThread == null) {
            // 成员方法可以直接调用内部类
            // 静态方法调用静态内部类
            this.heartbeatThread = new Thread(new HeartbeatTask(), "heartbeat-task");
            // 这里需要指定为守护线程，监听程序应该一直处于运行状态，不能影响整个程序的执行完成
            this.heartbeatThread.setDaemon(true);
            this.heartbeatThread.start();
        }
    }

    // 更新候选人
    @Override
    public void updateCandidate(Candidate candidate) {
        // 没有候选人，则报异常
        if (candidate == null || candidate.getEndpointCount() == 0) {
            throw new IllegalArgumentException("Invalid candidate: " + candidate);
        }
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
            // 使用Set集合遍历
            // for(Endpoint endpoint:candidate.getEndpointSet())
            // 这里使用了Iterable实现
            for (Endpoint endpoint : candidate) {
                isEndpointOnline = endpoint.isOnline();
                if (doDetect(endpoint) != isEndpointOnline) {
                    endpoint.setOnline(!isEndpointOnline);
                }
            }
        }

        /**
         * 检测节点是否在线，就是检测进程是否挂掉
         */
        private boolean doDetect(Endpoint endpoint) {
            return true;
        }
    }
}
