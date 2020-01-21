package com.conan.bigdata.common.nio;

import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

/**
 * 零拷贝
 * {@link java.nio.channels.FileChannel}
 * 这种操作系统层面的硬件操作，基本上都是调用 native 方法执行
 */
public class ZeroCopy {

    public static void main(String[] args) {

    }

    /**
     * 通过网络把一个文件从client 零拷贝传到server
     * 主要是客户端发送数据
     */
    private static void clientZeroCopy() throws Exception {
        SocketAddress socket = new InetSocketAddress("", 1111);
        SocketChannel sc = SocketChannel.open();
        sc.connect(socket);
        sc.configureBlocking(true);

        FileChannel from = new FileInputStream("").getChannel();
        long position = 0;
        long count = from.size();
        // 文件的Channel直接发送到套接字的Channel
        from.transferTo(position, count, sc);

        sc.close();
        from.close();
    }

    /**
     * 文件到文件 disk-to-disk 零拷贝
     */
    private static void diskZeroCopy() throws Exception {
        FileChannel from = new FileInputStream("").getChannel();
        FileChannel to = new FileInputStream("").getChannel();

        long position = 0;
        long count = from.size();

        from.transferTo(position, count, to);
        to.transferFrom(from, position, count);

        from.close();
        to.close();
    }
}