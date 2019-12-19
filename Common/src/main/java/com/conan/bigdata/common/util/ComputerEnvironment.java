package com.conan.bigdata.common.util;

import org.apache.commons.lang3.SystemUtils;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.*;
import java.util.*;

/**
 */
public class ComputerEnvironment {

    public static void main(String[] args) throws Exception {
        System.out.println("************** 电脑环境信息 *********************************************");
        show1();
        System.out.println("\n************** IP和MAC *********************************************");
        show2();
        System.out.println("\n************** JVM环境信息 *********************************************");
        show3();
    }

    // 获取电脑环境信息
    public static void show1() {
        Map<String, String> environments = System.getenv();
        for (Map.Entry<String, String> env : environments.entrySet()) {
            System.out.println(env.getKey() + " = " + env.getValue());
        }

        // 可以直接使用common-lang3包直接调用, 甚至可以判断是什么版本的系统
        System.out.println("is_windows : Yes ? " + SystemUtils.IS_OS_WINDOWS);
        System.out.println("is_windows_7 : Yes ? " + SystemUtils.IS_OS_WINDOWS_7);
        System.out.println("is_windows_8 : Yes ? " + SystemUtils.IS_OS_WINDOWS_8);
        System.out.println("is_linux : Yes ? " + SystemUtils.IS_OS_LINUX);
    }

    // 计算机对应的ip和mac的操作
    public static void show2() throws SocketException {
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
            NetworkInterface networkInterface = networkInterfaces.nextElement();
            if (networkInterface.isLoopback() || networkInterface.isVirtual() || networkInterface.isPointToPoint() || !networkInterface.isUp()) {
                System.out.println("无效:" + networkInterface.getName() + "\t" + networkInterface.getDisplayName() + "\t" + networkInterface.isLoopback() + "\t" + networkInterface.isVirtual() + "\t" + networkInterface.isPointToPoint() + "\t" + networkInterface.isUp());
            } else {
                System.out.println("有效:" + networkInterface.getName() + "\t" + networkInterface.getDisplayName());
                // 打印mac
                byte[] mac = networkInterface.getHardwareAddress(); // 字节数组, mac地址6个字节， 48bit， 所以该mac变量length = 6
                if (mac != null) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("    MAC: ");
                    for (int i = 0; i < mac.length; i++) {
                        sb.append(String.format("%02X%s", mac[i], i < mac.length - 1 ? "-" : ""));
                    }
                    System.out.println(sb.toString());
                }

                // 打印ip
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                InetAddress ip = null;
                System.out.print("    该网卡上的ip: ");
                while (inetAddresses.hasMoreElements()) {
                    ip = inetAddresses.nextElement();
                    if (ip instanceof Inet4Address) {
                        Inet4Address ip4 = (Inet4Address) ip;
                        System.out.print("ip4:" + ip4.getHostAddress() + "\t");
                    } else if (ip instanceof Inet6Address) {
                        Inet6Address ip4 = (Inet6Address) ip;
                        System.out.print("ip6:" + ip4.getHostAddress() + "\t");
                    } else {
                        System.out.println("无效ip");
                    }
                }
                System.out.println();
            }
        }
    }

    // JVM环境信息和运行内存情况
    public static void show3() throws InterruptedException {
        Properties properties = System.getProperties();
        Set<Map.Entry<Object, Object>> entries = properties.entrySet();
        for (Map.Entry<Object, Object> entry : entries) {
            System.out.println(String.valueOf(entry.getKey()) + " = " + String.valueOf(entry.getValue()));
        }

        // JVM运行信息 ManagementFactory是一个为我们提供各种获取JVM信息的工厂类，使用ManagementFactory可以获取大量的运行时JVM信息，比如JVM堆的使用情况，以及GC情况，线程信息等
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        String name = runtimeMXBean.getName(); // 虚拟机名称
        String specVendor = runtimeMXBean.getSpecVendor(); // 返回 Java 虚拟机规范供应商。
        long startTime = runtimeMXBean.getStartTime(); // Java 虚拟机的启动时间(以毫秒为单位)
        long uptime = runtimeMXBean.getUptime(); // Java 虚拟机的正常运行时间
        System.out.println(name.hashCode());
        // 虚拟机名称 生成规则 : jvm的进程id@主机名
        System.out.println("虚拟机名称:" + name + " 供应商:" + specVendor + " 启动时间:" + startTime + " 运行时间:" + uptime);
    }
}