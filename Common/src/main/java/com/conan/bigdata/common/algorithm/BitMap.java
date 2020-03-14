package com.conan.bigdata.common.algorithm;

import java.util.Arrays;
import java.util.BitSet;

/**
 * bitmap的实现可以用 int 数组(32bit)， 也可以用 byte 数组(8bit)， 仅仅是bit数不同， 逻辑一致
 * <p>
 * 可以衍生出 Bloom Filter 布隆过滤
 */
public class BitMap {

    private static final int N = 1001;

    public static void main(String[] args) {
        System.out.println("************* int 型的数组来表示 bitmap， 以32bit为准 ********************");
        integerBitMap();

        System.out.println("\n************* byte 型的数组来表示 bitmap， 以8bit为准 ********************");
        byteBitMap();

        System.out.println("\n************* 进制转换 ********************");
        // 打印整数的二进制
        System.out.println(Integer.toBinaryString(10000000));
        System.out.println(Long.toBinaryString(5000000000L));

        // 二进制转十进制
        System.out.println(Integer.parseInt("100110001001011010000000", 2));
        System.out.println(Long.parseLong("100101010000001011111001000000000", 2));

        System.out.println("\n************* java.util.BitSet实现 ********************");
        int[] array = new int[]{1, 2, 3, 22, 0, 3, 63};
        BitSet bitSet = new BitSet(10); // 构造函数参数表示要构建多少bit的bitmap， 类似于自己实现的数组表示法 arr.length * 8
        int size = bitSet.size(); // 底层是 Long 类型实现， size 为 64 的倍数
        System.out.println("bit size = " + size);
        for (int i = 0; i < array.length; i++) {
            bitSet.set(array[i], true);
        }
        System.out.println(bitSet.size());
        for (int i = 0; i < bitSet.size(); i++) {
            System.out.print(bitSet.get(i) ? 1 : 0);
        }
        System.out.println("\n总共有多少数字 = " + bitSet.cardinality());

        System.out.println("\n************* 生成bitmap数据集合 ********************");
        storeMultiNum();
    }

    private static void storeMultiNum() {
        int[] a = new int[1 + 100 / 32];
        putNumToBitMap(a, 23);
        putNumToBitMap(a, 79);
        putNumToBitMap(a, 91);
        putNumToBitMap(a, 23); // 这里 23 和之前的23重复了，所以从打印的bit看，只有4个数, 明显是一个比较好的去重方式，只要遍历一次数据即可
        putNumToBitMap(a, 50);
        System.out.println(Arrays.toString(a));
        String[] binaryStr = new String[a.length];
        for (int i = 0; i < binaryStr.length; i++) {
            binaryStr[i] = Integer.toBinaryString(a[i]);
        }
        System.out.println(Arrays.toString(binaryStr));
    }

    private static void putNumToBitMap(int[] a, int num) {
        int index = num >> 5;
        int position = num & 0x1F;
        a[index] |= (1 << position);
    }

    private static void integerBitMap() {
        /**
         * int[] a = new int[1 + N / 32];  N 表示要参与统计的数的范围， 一般是这批数里面的最大值
         * 注意 java是有符号数， 如果这里n = 31的时候， 得到的二进制 1 后面跟31个 0， 这里的1表示负数，该int值是 Integer.MIN_VALUE
         */
        int[] a = new int[1 + N / 32];
        // 计算数字num在int[]中的位置（num/8和num >> 5一样），也就是说num在int[k]，算这个k是几
        int index = N >> 5;
        // 计算数字num在int[index]中的位置，就是在int[index]的第几位，每个int有32位（num % 32）
        int position = N & 0x1F; // 等同于 n % 32

        // 如果是第一次生成这个bitmap数组，需要和之前的数据合并
        a[index] |= (1 << position);
        System.out.println(index);
        System.out.println(a[index]);
        System.out.println(Integer.toBinaryString(a[index]));
    }

    private static void byteBitMap() {
        /**
         * byte[] a = new byte[1 + N / 8];  N 表示要参与统计的数的范围， 一般是这批数里面的最大值
         * 注意 java是有符号数， 如果这里n = 7的时候， 得到的二进制 1 后面跟7个 0， 这里的1表示负数，该byte值是 Byte.MIN_VALUE
         */
        byte[] a = new byte[1 + N / 8];
        int index = N >> 3;
        int position = N & 0x07; // 等同于 n % 7

        a[index] |= (1 << position);
        System.out.println(index);
        System.out.println(a[index]);
        // byte 数用 int 表示
        System.out.println(Integer.toBinaryString(a[index]));
    }
}
