package com.conan.bigdata.common.concurrent.thread;

/**
 * 多线程环境下的单例演示，兼顾性能
 * 1. 饿汉模式
 * 2. 双重检验锁 + volatile
 */
public class SingletonExp {

    private static volatile SingletonExp INSTANCE = null;
    // static 关键字能保证当类初始化时，能给予初始值，而不是默认值（null），且保证这个对象已经完全初始化完毕
    private static final SingletonExp INSTANCE1=new SingletonExp();

    // 私有构造方法，用于内部创建实例
    private SingletonExp() {

    }

    // 立即加载
    /**
     * 饿汉模式
     * 该实例是随着类加载是一起初始化的，属于立即加载，只能是使用私有构造方法
     * 注意这种机制的前提是不能被某个线程修改，如果有修改的必要，同样需要加锁或者volatile
     */
    public static SingletonExp getInstance(){
        return INSTANCE1;
    }

    // 下面的四个方法是延迟加载

    /**
     * 这种最简单，单线程环境下可以实现单例
     * 多线程环境下则可能不满足要求，如果两个线程同时执行到if语句，则都满足条件，那么两个线程分别要创建一个实例，与单例模式不符
     */
    public static SingletonExp getInstance1() {
        if (INSTANCE == null) { // 1
            INSTANCE = new SingletonExp();  // 2
        }
        return INSTANCE;
    }

    /**
     * 考虑上面的问题，加上同步块如何
     * 可以满足单例模式的功能，多线程环境下也可以满足要求
     * 但是，每次访问都需要加锁，上下文切换，性能不高
     */
    public static SingletonExp getInstance2() {
        synchronized (SingletonExp.class) {
            if (INSTANCE == null) {
                INSTANCE = new SingletonExp();
            }
        }
        return INSTANCE;
    }

    /**
     * 进一步完善，使用双重检查锁定，注意：这个容易出错
     * 看上去能安全实现高性能的单例，实则不然，考虑重排序就可能有问题
     * synchronized 保证原子性和可见性，创建对象的过程，可以分为三个步骤：1.分配内存空间 2.执行构造函数初始化对象 3.内存空间的引用赋值给共享变量
     * 因为synchronized不保证临界区的代码有序性，所以三个步骤很可能执行顺序为 1 -> 3 -> 2
     * 这样就意味着可能存在一种场景：构造函数还没有执行完毕，对象还没有完整的初始化，但是共享变量已经不为 null
     * 由于可见性的问题，其它线程执行到第一个if条件时，发现不为null，直接返回INSTANCE，实际上它还没有完全初始化完毕，可能造成程序出错
     * <p>
     * 解决方法： volatile 可以禁止指令重排序
     */
    public static SingletonExp getInstance3() {
        if (INSTANCE == null) {
            synchronized (SingletonExp.class) {
                if (INSTANCE == null) {
                    INSTANCE = new SingletonExp();
                }
            }
        }
        return INSTANCE;
    }


    /**
     * 更简单的方法： 利用类的静态变量只会在类初始化时创建一次的特点，这个也是延迟加载
     */
    private static class InstanceHolder {
        static final SingletonExp EXP = new SingletonExp();
    }

    public static SingletonExp getInstance4() {
        return InstanceHolder.EXP;
    }
}
