package test;

/**
 * Created by Administrator on 2017/4/25.
 */
public class AAA {

    public static int[] a = new int[1 + 10 / 32];

    public static void main(String[] args) throws InterruptedException {
        AAA a=new AAA();
        System.out.println(a.getClass().getPackage().getName());
    }
}
