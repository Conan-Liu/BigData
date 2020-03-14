package com.conan.bigdata.common.algorithm;


import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * bitmap 占用空间太大
 * 调用 {@link org.roaringbitmap.RoaringBitmap} 压缩bitmap
 * 参考 https://github.com/RoaringBitmap/RoaringBitmap
 */
public class CompressBitmap {

    public static void main(String[] args) {
        show1();
        show2();
        show3();
    }

    private static void show1() {
        // 向RoaringBitmap中添加1，2，3，3，1000五个数字，这里有两个3，只会记录一次，从打印可知
        RoaringBitmap roaringBitmap1 = RoaringBitmap.bitmapOf(1, 2, 3, 3, 1000);
        // 添加一个数字
        roaringBitmap1.add(2);
        roaringBitmap1.add(4);
        // 打印RoaringBitmap中包含的数字
        System.out.println(roaringBitmap1);
        // 创建另一个空的RoaringBitmap
        RoaringBitmap roaringBitmap2 = new RoaringBitmap();
        // 向空的RoaringBitmap添加[1000,1099)共2000个数字
        roaringBitmap2.add(1000L, 1099L);
        // 反回第3个数字是1000，第0个数字是1，第1个数字是2，以此类推
        System.out.println("1. " + roaringBitmap1.select(3));
        // 返回value=2时的索引为2，value=1时索引为1
        System.out.println("2. " + roaringBitmap1.rank(1));
        // 判断是否包含1000
        System.out.println("3. " + roaringBitmap1.contains(1000));
        // 判断是否包含7
        System.out.println("4. " + roaringBitmap1.contains(7));

        // 两个RoaringBitmap进行or操作，数值合并，产生新的RoaringBitmap
        RoaringBitmap roaringBitmap3 = RoaringBitmap.or(roaringBitmap1, roaringBitmap2);
        // roaringBitmap1和roaringBitmap2进行位运算，并将值赋给roaringBitmap1
        roaringBitmap1.or(roaringBitmap2);
        // 判断oaringBitmap1和RoaringBitmap3是否相等，显然相等的
        boolean equals = roaringBitmap3.equals(roaringBitmap1);
        if (!equals) {
            throw new RuntimeException("bug");
        }
        // 查看roaringBitmap1中存了多少数字，1，2，3，1000和[10000,12000)，共2004个数字
        long cardinality = roaringBitmap1.getLongCardinality();
        System.out.println("bitmap 数字总数 = " + cardinality);

        System.out.println(roaringBitmap1);
    }

    // RoaringBitmap 创建一个最大容量的bitmap
    private static void show2() {
        RoaringBitmap roaringBitmap = new RoaringBitmap();
        roaringBitmap.add(100000000);
        // 这一个数也就占16字节
        long byteSize1 = roaringBitmap.getLongSizeInBytes();
        System.out.println("byteSize1 size = " + byteSize1);

        roaringBitmap.add(0L, 1L << 32);
        long count = roaringBitmap.getLongCardinality();
        System.out.println("count = " + count);
        if (count != (1L << 32)) {
            throw new RuntimeException("bug!");
        }

        // 从代码运行结果可知，42亿个数就占用了 655368B ， 没明白为什么这么少
        long byteSize2 = roaringBitmap.getLongSizeInBytes();
        System.out.println("byteSize2 size = " + byteSize2);
    }

    // 大数据环境中，数据需要在各个机器上传输，需要序列化反序列化
    private static void show3() {
        RoaringBitmap roaringBitmap1 = RoaringBitmap.bitmapOf(1, 2, 3, 1000);
        System.out.println(roaringBitmap1);
        roaringBitmap1.runOptimize();
        byte[] array = new byte[roaringBitmap1.serializedSizeInBytes()];
        // 序列化
        roaringBitmap1.serialize(ByteBuffer.wrap(array,0,array.length));
        roaringBitmap1.clear();

        // 反序列化
        RoaringBitmap roaringBitmap2 = new RoaringBitmap();
        try {
            roaringBitmap2.deserialize(ByteBuffer.wrap(array,0,array.length));
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean equals=roaringBitmap2.equals(roaringBitmap1);
        if (equals) {
            throw new RuntimeException("bug");
        }
        System.out.println(roaringBitmap2);
    }
}