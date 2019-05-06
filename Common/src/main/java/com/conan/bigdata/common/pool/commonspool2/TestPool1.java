package com.conan.bigdata.common.pool.commonspool2;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Created by Administrator on 2019/5/6.
 */
public class TestPool1 {

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        GenericObjectPoolConfig<Connection> config = new GenericObjectPoolConfig<>();
        // 设置最大链接数
        config.setMaxTotal(5);
        // 设置获取链接超时时间
        config.setMaxWaitMillis(1000);
        ConnectionPool connectionPool = new ConnectionPool(factory, config);

        for (int i = 0; i < 10; i++) {
            Connection conn = connectionPool.borrowObject();
            System.out.println("borrow a connection : " + conn + " active connection : " + connectionPool.getNumActive());
            // 下面这段代码是归还链接的， 如果不归还， 最大链接数是5个， 已经到达5个的时候但是不归还的话， 后面还想创建就不行了，造成链接泄露
            // 然后超时时间 1000ms 到了， 就报错了， 超时异常， 放开注释， 让它能成功归还就能正确执行
            connectionPool.returnObject(conn);

        }
    }
}