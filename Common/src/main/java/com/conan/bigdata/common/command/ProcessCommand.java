package com.conan.bigdata.common.command;


import java.io.*;

/**
 * Java支持用代码来提交命令到Linux服务器上执行
 * 1. Process p = Runtime.getRuntime().exec()
 * 该类提供三个流来处理输入输出错误，输入输出都是从父进程角度观察
 * getInputStream() 标准输入
 * getOutputStream() 标准输出
 * getErrorStream() 标准错误
 * 这里有个坑，不能用同步方法来处理这个流，否则可能会出现某个流没有被及时处理，一直占用缓冲区，
 * 等缓冲区满了，无法写入数据，导致线程阻塞，进程卡死，对外现象就是进程无法停止，也不占资源，什么反应也没有
 * 解决方法：用多线程来处理流，一个线程处理一个，大家互不干涉
 */
public class ProcessCommand {

    private static class ProcessStreamRunable implements Runnable {
        private BufferedReader br;
        private String logType;

        private ProcessStreamRunable(InputStream in, String logType) {
            this.br = new BufferedReader(new InputStreamReader(in));
            this.logType = logType;
        }

        @Override
        public void run() {
            try {
                if ("INPUT".equals(logType)) {
                    System.out.println("INPUT LOG:");
                } else if ("ERROR".equals(logType)) {
                    System.out.println("ERROR LOG:");
                }
                String inLine;
                while ((inLine = br.readLine()) != null) {
                    System.out.println(inLine);
                }
                br.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 错误写法，错误写法，错误写法
     * 这是同步处理，一定不能这么写，代码写在后面的流，无法及时处理，缓冲区写满，子进程里的线程阻塞挂起
     */
    public static void syncProcess() throws Exception {
        Process process = Runtime.getRuntime().exec("java -jar /home/hadoop/temp/etl_work/spark-0.0.1-SNAPSHOT.jar");
        BufferedReader bin = new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedWriter bout = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
        BufferedReader berr = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        String line = "";
        System.out.println("下面是CallMain的输出");
        System.out.println("\n子程序的输出内容如下:");
        while ((line = bin.readLine()) != null) {
            System.out.println(line);
        }
        bin.close();
        int subProcessExitCode = process.waitFor();
        System.out.println("子程序返回值: " + subProcessExitCode);
        if (subProcessExitCode > 0) {
            throw new Exception("子程序报异常退出了");
        } else {
            System.out.println("Happy Ending");
        }
    }


    /**
     * 正确的写法，利用多线程来处理流，一个线程一个
     */
    public static void multiThreadProcess() throws Exception {
        Process pro = Runtime.getRuntime().exec(new String[]{"ls", "-l", "/"});

        Thread t1 = new Thread(new ProcessStreamRunable(pro.getInputStream(), "INPUT"));
        Thread t2 = new Thread(new ProcessStreamRunable(pro.getErrorStream(), "ERROR"));
        t1.start();
        t2.start();

        int subProcessExitCode = pro.waitFor();
        System.out.println("子程序返回值: " + subProcessExitCode);
        if (subProcessExitCode > 0) {
            throw new Exception("子程序报异常退出了");
        } else {
            System.out.println("Happy Ending");
        }
    }


    /**
     * 只能提交简单命令，可以通过如下形式来处理复杂命令
     * <p>
     * 通过 /bin/sh -c "command" 格式来提交复杂命令，如管道命令
     */
    public static void execComplexCommand() {
        boolean isKettleExecute = false;
        try {
            String command = "ls -l ~ | grep 00";
            System.out.println(command);
            Process process = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", command});
            BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String out = in.readLine();
            System.out.println("out: " + out);
            if (out == null || "".equals(out)) {
                isKettleExecute = false;
            } else {
                isKettleExecute = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(isKettleExecute);
    }


    public static void main(String[] args) throws Exception {
        multiThreadProcess();
    }

}
