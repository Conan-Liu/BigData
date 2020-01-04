package com.conan.bigdata.common.gc;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * -X 参数
 * 以这个开头的参数是非标准参数，也就是只能被部分VM识别，而不能被全部VM识别的参数
 * -Xms1024m  堆最小值        =  -XX:MaxHeapSize=1024m
 * -Xmx2048m  堆最大值        =  -XX:MaxHeapSize=2048m
 * -Xss10m    线程栈大小
 * -Xint      指定java解释执行代码, 样例： 执行java -Xint -version，可以看到interpreted mode
 * -Xcomp     指定java编译代码， 样例： 执行java -Xcomp -version，可以看到compiled mode
 * -Xmixed    默认混合模式，先编译后执行， 执行java -version， 可以看到mixed mode
 * <p>
 * <p>
 * <p>
 * -XX 参数
 * 以这个开头的参数是非稳定参数，随时可能被修改或者移除
 * 格式： -XX:+ 或者 -XX:-  (+ 表示开启， - 表示关闭)
 * -XX:+PrintGCDetails  -XX:-PrintGCDetails  是否打印GC收集细节
 * -XX:+UseSerialGC     -XX:-UserSerialGC    是否使用串行垃圾收集器
 */
class Person {
    /**
     * 作为对象来测试内存存储情况
     */
    private int id;
    private String name;
    private double height;
    private double weight;
    private char gender;
}

public class GCResearch {

    public static void main(String[] args) {

//        System.out.println("*************** 堆内存溢出示例 **************************");
//        heapOutOfMemory();

//        System.out.println("*************** 栈内存溢出示例 **************************");
//        stackOverFlow();

//        System.out.println("*************** 方法区内存溢出示例 **************************");
//        constantOutOfMemory();

//        System.out.println("*************** 非JVM内存溢出示例 **************************");
//        directOutOfMemory();

        System.out.println("*************** 验证Java配置的opt参数示例 **************************");
        checkJVMopts();
    }

    /**
     * 堆内存溢出(存储所有对象，数组，一个JVM只有一个heap区域，被所有线程共享, 数据不是线程安全的，分成年轻代，老年代，方法区(永久代))
     * 为了演示，这个值设置的很小，编译尽早报内存溢出的错误， 注意内存溢出是 Error 类型的， 不是Exception异常
     * JVM参数
     * -verbose:gc -Xms10m -Xmx10m -XX:+PrintGCDetails
     * 错误格式
     * java.lang.OutOfMemoryError: Java heap space
     * GC 机制就是针对 heap 的
     */
    private static void heapOutOfMemory() {
        List<Person> persons = new ArrayList<>();
        int counter = 0;
        while (true) {
            persons.add(new Person());
            System.out.println("Instance: " + (counter++));
        }
    }


    /**
     * 栈内存溢出(线程私有，存储局部变量，分三大部分：局部变量区，执行环境上下文，操作指令区)
     * 每个线程有自己的线程私有区， stack就是线程私有的
     * 为了演示，这个值设置的很小，编译尽早报内存溢出的错误， 注意内存溢出是 Error 类型的， 不是Exception异常
     * JVM参数
     * -verbose:gc -Xss110k -XX:MaxDirectMemorySize=5m -XX:+PrintGCDetails
     */
    private static int counter = 0;

    private static void count() {
        counter++;
        count();
    }

    private static void stackOverFlow() {
        try {
            count();
        } catch (Exception e) {
            System.out.println("the stack frame depth is : " + counter);
            e.printStackTrace();
            throw e;
        }
    }


    /**
     * 方法区内存溢出(存放类，静态变量，静态方法，常量，成员方法，被所有线程共享, 数据不是线程安全的)
     * jdk1.7以后方法区位于堆，就是永久代Permanent
     * JVM参数
     * -verbose:gc -Xms10m -Xmx10m -XX:+PrintGCDetails
     * 错误格式
     * java.lang.OutOfMemoryError: Java heap space
     */
    private static void constantOutOfMemory() {
        List<String> strs = new ArrayList<>();
        int counter = 0;
        while (true) {
            // String 类的 intern 方法的作用是把字符串加载到常量池中
            strs.add(String.valueOf("这是数字: " + counter++).intern());
        }
    }


    /**
     * 非JVM内存溢出
     * <p>
     * 错误格式
     * java.lang.OutOfMemoryError: Java heap space
     */
    private static void directOutOfMemory() {
        int count = 1;
        while (true) {
            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024 * 1024);
            count++;
        }
    }


    /**
     * 在Terminal使用JDK工具查看是否配置JVM参数
     * jps -l 表示查看java运行的进程号
     * jinfo -flag 配置项 进程号，例子： jinfo -flag PrintGCDetails pid表示查看JVM是否配置PrintGCDetails参数，jinfo -flag MetaspaceSize pid， 查看MetaspaceSize参数，以此类推
     * jinfo -flags pid 打印JVM的详细信息
     * jstat -gcutil pid interval(ms) 打印实时信息
     */
    private static void checkJVMopts() {
        try {
            TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}