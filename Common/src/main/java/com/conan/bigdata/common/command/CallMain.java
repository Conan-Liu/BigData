package com.conan.bigdata.common.command;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/**
 */
public class CallMain {

    public static void main(String[] args) throws Exception {
        Process process = null;
        process = Runtime.getRuntime().exec("java -jar /home/hadoop/temp/etl_work/spark-0.0.1-SNAPSHOT.jar");
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
        }else{
            System.out.println("Happy Ending");
        }
    }

}