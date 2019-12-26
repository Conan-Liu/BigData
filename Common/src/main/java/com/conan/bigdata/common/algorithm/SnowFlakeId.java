package com.conan.bigdata.common.algorithm;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

/**
 * Created by Administrator on 2019/1/24.
 * <p>
 * 实现递增的唯一键， 数字类型， 比UUID占用较少的空间， 生成速度快， 还不能做到指定自增步长
 */
public class SnowFlakeId {

    // 开始时间戳 2019-01-01
    private static final long EPOCH = 1546272000000L;

    /**
     * 每一部分占用的位数
     */
    // 机器ID所占位数
    private static final int WORKER_ID_BITS = 5;
    // 数据标识所占位数
    private static final int DATA_CENTER_ID_BITS = 5;
    // 序列在id中占的位数
    private static final int SEQ_BITS = 12;

    /**
     * 每一部分的最大值
     */
    // 支持最大机器id， 机器id从 0 开始
    private static final int MAX_WORKER_ID = -1 ^ (-1 << WORKER_ID_BITS);
    // 支持最大的数据标识id
    private static final int MAX_DATA_CENTER_ID = -1 ^ (-1 << DATA_CENTER_ID_BITS);
    // 生成序列掩码， 序列最大值， 默认4095， 总共4096个数字
    private static final int MAX_SEQ = -1 ^ (-1 << SEQ_BITS); // = 4095 ,  如果改为 1 << SEQ_BITS ， 则 = 4096

    /**
     * 每一部分向左的位移
     */
    // 机器ID向左移12位
    private static final int WORKER_ID_SHIFT = SEQ_BITS;
    // 数据标识id向左移17位
    private static final int DATA_CENTER_ID_SHIFT = SEQ_BITS + WORKER_ID_BITS;
    // 时间戳左移 22 位
    private static final long TIMESTAMP_SHIFT = SEQ_BITS + WORKER_ID_BITS + DATA_CENTER_ID_BITS;

    /**
     * 传入的参数
     */
    // 工作机器id
    private int workId;
    // 数据中心id
    private int dataCenterId;
    // 毫秒内序列
    private int seq = 0;
    // 上次生成id的时间戳
    private long lastTimestamp = -1l;

    public SnowFlakeId(int workId, int dataCenterId) {
        if (workId > MAX_WORKER_ID || workId < 0) {
            throw new IllegalArgumentException(String.format("work id can't be greater than %d or less than 0 !", MAX_WORKER_ID));
        }
        if (dataCenterId > MAX_DATA_CENTER_ID || dataCenterId < 0) {
            throw new IllegalArgumentException(String.format("data center id can't be greater than %d or less than 0 !", MAX_DATA_CENTER_ID));
        }

        this.workId = workId;
        this.dataCenterId = dataCenterId;
    }

    /**
     * 获得下一个ID (该方法是线程安全的)
     *
     * @return SnowflakeId
     */
    public synchronized long nextId() {
        long timestamp = System.currentTimeMillis();

        // 如果当前时间小于上一次ID生成的时间戳，说明系统时钟回退过这个时候应当抛出异常
        if (timestamp < lastTimestamp) {
            // 这里可以没必要直接抛异常退出， 阻塞到获取的时间正常即可
            timestamp = getNextMillis(lastTimestamp);
            throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp));
        }

        // 如果是同一时间生成的，则进行毫秒内序列
        if (timestamp == lastTimestamp) {
            seq = (seq + 1) & MAX_SEQ;
            // 毫秒内序列溢出
            if (seq == 0) {
                // 阻塞到下一个毫秒,获得新的时间戳
                timestamp = getNextMillis(lastTimestamp);
            }
        } else {
            seq = 0;
        }

        lastTimestamp = timestamp;
        // | 表示或
        return ((timestamp - EPOCH) << TIMESTAMP_SHIFT)
                | (dataCenterId << DATA_CENTER_ID_SHIFT)
                | (workId << WORKER_ID_SHIFT)
                | seq;
    }

    /**
     * 阻塞到下一个毫秒，直到获得新的时间戳
     *
     * @param lastTimestamp 上次生成ID的时间截
     * @return 当前时间戳
     */
    private long getNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }

    /**
     * 正常情况下，一台服务器可以运行多个JVM，就相当于一台服务器有多个work节点，那么该服务器可以认为是一个DataCenter
     * 很多时候一台服务器会有多个网卡，那么就意味着有多个ip，我们暂定一个ip对应一个DataCenter， 任务的提交也是按照ip分发的
     * 这里根据ip来计算DataCenterId， 可以使用ip转换成对应的整数取模, 具体操作参考
     * {@link com.conan.bigdata.hive.udf.GetIpAttr.convertIP}
     * 当然，这里是取模，那么意味着很多机器的时候，可能会得到同一个 dataCenterId， 这里的maxDataCenterId和集群机器数一致就避免了
     *
     * @return
     */
    private int getDataCenterId() {
        return 0;
    }

    /**
     * 每台机器上可以运行多个JVM，那么我们暂且认为一个JVM就是一个worker，这样不同的JVM就是不同的worker，互不干扰
     *
     * @return
     */
    private int getWorkId() {
        RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
        String jvmName = bean.getName(); // jvm的名称格式: jvm进程id@主机名
        // String 的 hashCode 可能为负数
        return Math.abs(jvmName.hashCode()) % (MAX_WORKER_ID + 1);
    }

    public static void main(String[] args) {
        SnowFlakeId idWorker = new SnowFlakeId(0, 0);
        long start = System.currentTimeMillis();
        // 测试生成速度 1亿条  24秒
        for (int i = 0; i < 10; i++) {
            long id = idWorker.nextId();
            System.out.println(id + " = " + Long.toBinaryString(id));
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}