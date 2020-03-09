package com.conan.bigdata.common.command;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * java 提交 Linux管道命令
 */
public class PipeCommand {
    public static void main(String[] args) {
        boolean isKettleExecute = false;
        try {
            String command = String.format("ps -ef | awk -v awkVar=%d '$2==awkVar{print}' | grep kettle | grep -v grep", 26575);
            System.out.println(command);
            Process process = Runtime.getRuntime().exec(command);
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
}