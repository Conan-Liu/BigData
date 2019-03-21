package test;

/**
 * Created by Administrator on 2019/3/20.
 */
public class LcyTest {
    public static void main(String[] args) {
        double a =0.1;
        double b =2.0-1.9;
        System.out.println(a);
        System.out.println(b);
        System.out.println(a==b);
        System.out.println(Math.abs(a-b));
        System.out.println(Math.abs(a-b)<1e-6);
    }
}