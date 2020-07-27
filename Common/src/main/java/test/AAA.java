package test;

import java.io.IOException;

public class AAA {

    private static class T1 implements Runnable{

        @Override
        public void run() {
            while (true) {
                try {
                    System.out.println("t1");
                    Thread.sleep(20000);
                    System.exit(0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static class T2 implements Runnable{

        @Override
        public void run() {
            while(true){
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("aaaa");
            }
        }
    }

    public static void main(String[] args) throws IOException {

        Thread t1=new Thread(new T1(),"t1");
        Thread t2=new Thread(new T2(),"t2");
        t1.start();
        t2.start();


        System.out.println(10^10);
    }

}
