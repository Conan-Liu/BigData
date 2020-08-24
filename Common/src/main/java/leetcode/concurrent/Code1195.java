package leetcode.concurrent;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public class Code1195 {

    private static final int n = 10;
    private static Lock lock = new ReentrantLock();
    private static Condition c1 = lock.newCondition();
    private static Condition c2 = lock.newCondition();
    private static Condition c3 = lock.newCondition();
    private static Condition c4 = lock.newCondition();
    private static int i = 1;

    private static class Fizz implements Runnable {

        @Override
        public void run() {
            lock.lock();
            try {
                while (i <= n) {
                    if (i % 3 == 0) {
                        System.out.print("fizz");
                        i++;
                        if (i % 15 == 0) {
                            c3.signal();
                        } else if (i % 5 == 0) {
                            c2.signal();
                        } else {
                            c4.signal();
                        }
                    }
                    c1.await();
                    if(i>n){
                        c1.signal();
                        c2.signal();
                        c3.signal();
                        c4.signal();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }
    }

    private static class Buzz implements Runnable {

        @Override
        public void run() {
            lock.lock();
            try {
                while (i <= n) {
                    if (i % 5 == 0) {
                        System.out.print("buzz");
                        i++;
                        if (i % 15 == 0) {
                            c3.signal();
                        } else if (i % 3 == 0) {
                            c1.signal();
                        } else {
                            c4.signal();
                        }
                    }
                    c2.await();
                    if(i>n){
                        c1.signal();
                        c2.signal();
                        c3.signal();
                        c4.signal();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }
    }

    private static class FizzBuzz implements Runnable {

        @Override
        public void run() {
            lock.lock();
            try {
                while (i <= n) {
                    if (i % 15 == 0) {
                        System.out.print("fizzbuzz");
                        i++;
                        if (i % 5 == 0) {
                            c2.signal();
                        } else if (i % 3 == 0) {
                            c1.signal();
                        } else {
                            c4.signal();
                        }
                    }
                    c3.await();
                    if(i>n){
                        c1.signal();
                        c2.signal();
                        c3.signal();
                        c4.signal();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }
    }

    private static class Number1 implements Runnable {

        @Override
        public void run() {
            lock.lock();
            try {
                while (i <= n) {
                    if (i % 3 == 0 || i % 5 == 0) {
                        if (i % 15 == 0) {
                            c3.signal();
                        } else if (i % 3 == 0) {
                            c1.signal();
                        } else if (i % 5 == 0) {
                            c2.signal();
                        }
                        c4.await();
                    }
                    System.out.print(i);
                    i++;
                    if(i>n){
                        c1.signal();
                        c2.signal();
                        c3.signal();
                        c4.signal();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }
    }

    public static void main(String[] args) {
        Thread t1 = new Thread(new Fizz());
        Thread t2 = new Thread(new Buzz());
        Thread t3 = new Thread(new FizzBuzz());
        Thread t4 = new Thread(new Number1());
        t1.start();
        t2.start();
        t3.start();
        t4.start();
    }
}
