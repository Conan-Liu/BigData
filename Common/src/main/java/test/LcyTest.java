package test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2019/3/20.
 */
public class LcyTest {

    class InnerClass{
        public void show(){
            System.out.println("bbbbbbbbbbb");
        }
    }
    static class StaticInnerClass{
        public void show(){
            System.out.println("aaaaaaaaaaa");
        }
    }

    public static void main(String[] args) {
        LcyTest lt=new LcyTest();
        InnerClass in=lt.new InnerClass();
        in.show();

        LcyTest.StaticInnerClass sic=new LcyTest.StaticInnerClass();
        sic.show();

    }
}