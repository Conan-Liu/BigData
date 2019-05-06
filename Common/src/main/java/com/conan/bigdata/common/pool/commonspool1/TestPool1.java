package com.conan.bigdata.common.pool.commonspool1;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Created by Administrator on 2019/5/6.
 */
public class TestPool1 {

    public static void main(String[] args) throws Exception {
        GenericObjectPoolConfig<Connection> config = new GenericObjectPoolConfig<>();
        // 设置最大链接数
        config.setMaxTotal(5);
        // 设置获取链接超时时间
        config.setMaxWaitMillis(1000);
        // 下面这两个参数检测对象是否有效，  默认false， 设置为true会影响性能
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        ConnectionFactory factory = new ConnectionFactory();
        GenericObjectPool<Connection> connectionPool = new GenericObjectPool<>(factory, config);

        for (int i = 0; i < 10; i++) {
            Connection conn = connectionPool.borrowObject();
            System.out.println(i + " time get " + conn.getV());
            connectionPool.returnObject(conn);

        }
    }
}