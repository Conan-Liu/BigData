package test;

/**
 * 远程代码调试
 * 代码最好保持一致，否则本地代码和远程任务执行情况有些出入，不能正常执行调试，容易迷惑
 *
 * 开启远程调试的JVM参数
 * -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=12345
 */
public class RemoteDebug {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("程序开始执行...");
        // 这里睡眠 10s 钟，能来得及启动idea的debug，不至于代码执行一闪而过
        Thread.sleep(10000);
        int n = 0;
        for (int i = 0; i < 100; i++) {
            n+=i;
        }
        System.out.println("遇到断点，暂停...");
        // 在代码前打上断点，然后F6单步执行调试
        System.out.println("n = " + n);
        System.out.println("单步调试1...");
        System.out.println("单步调试2...");
        System.out.println("单步调试3...");
        System.out.println("调试结束");
    }
}