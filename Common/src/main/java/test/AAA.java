package test;

/**
 * Created by Administrator on 2017/4/25.
 */
public class AAA {

    public static int[] a = new int[1 + 10 / 32];

    public static void main(String[] args) throws InterruptedException {
        System.out.println("下面开始暂停1000s：");
        Thread.sleep(1000000);
    }
}
