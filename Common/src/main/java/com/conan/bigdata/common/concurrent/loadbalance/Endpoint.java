package com.conan.bigdata.common.concurrent.loadbalance;

/**
 * 提供服务的下游节点，可能服务还在线，可能已经挂了
 */
public class Endpoint {

    private String host;
    private int port;
    private int weight;
    // 定义 volatile 变量来确认节点是否有效，可见性，保证集群内的节点能及时感知到该机器是否有效
    private volatile boolean isOnline=true;

    public Endpoint(String host,int port){
        this.host=host;
        this.port=port;
        // 默认权重， 先不考虑这个
        this.weight=5;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public boolean isOnline() {
        return isOnline;
    }

    public void setOnline(boolean online) {
        isOnline = online;
    }

    @Override
    public String toString() {
        return "Endpoint{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", weight=" + weight +
                ", isOnline=" + isOnline +
                '}';
    }

}
