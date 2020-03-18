package test;

/**
 * 验证抽象类也可以写main方法
 */
public abstract class TestAbstractMain {

    // 抽象类可以有私有方法，接口不行
    private static void test(){
        System.out.println("aaa");
    }
    public static void main(String[] args) {
        System.out.println("aaa");
    }
}