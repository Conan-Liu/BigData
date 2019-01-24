package com.conan.bigdata.common.util;

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