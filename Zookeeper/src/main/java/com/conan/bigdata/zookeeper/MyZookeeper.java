package com.conan.bigdata.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Conan on 2019/4/30.
 */
public class MyZookeeper {

    public static final String HOST_NAME = "CentOS";
    public static final int PORT = 2181;
    public static final String ZK_PATH = "/";

    public static void main(String[] args) {
        System.out.println("========打印节点的子节点========");
        printChildred();
        System.out.println();
        System.out.println("========打印节点的子节点========");
        printOffset();
    }

    // 打印节点的子节点
    public static void printChildred() {
        List<String> zkChildren;
        try {
            ZooKeeper zk = new ZooKeeper(HOST_NAME + ":" + PORT, 2000, null);
            zkChildren = zk.getChildren(ZK_PATH, false);
            System.out.println("Znodes of /:");
            for (String zkChild : zkChildren) {
                System.out.println(zkChild);
            }
        } catch (InterruptedException | KeeperException | IOException e) {
            e.printStackTrace();
        }
    }

    // 打印kafka的offset
    public static void printOffset() {
        try {
            ZooKeeper zk = new ZooKeeper(HOST_NAME + ":" + PORT, 2000, null);
            String data = new String(zk.getData("/kafka/consumers/test_group/offsets/kafkastreaming/0", false, null));
            System.out.println(data);
        } catch (InterruptedException | KeeperException | IOException e) {
            e.printStackTrace();
        }
    }
}