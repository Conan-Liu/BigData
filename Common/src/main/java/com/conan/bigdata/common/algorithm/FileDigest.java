package com.conan.bigdata.common.algorithm;

import com.conan.bigdata.common.util.Tools;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.MessageDigest;

/**
 * 摘要算法
 * 常用的是MD5，SHA-1计算文件摘要，实现文件的极速秒传
 * 先判断文件大小，大小不同则文件必然不同
 * 再判断摘要，摘要相同则文件相同
 */
public class FileDigest {

    private String getSHA1(InputStream in) {
        if (in == null) {
            return "";
        }
        MessageDigest digest = null;
        byte[] buffer = new byte[4096];
        int length;
        try {
            digest = MessageDigest.getInstance("SHA-1");
            while ((length = in.read(buffer)) != -1) {
                digest.update(buffer, 0, length);
            }
            return Tools.byteToHexString(digest.digest());
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    public static void main(String[] args) throws Exception{
        FileDigest digest=new FileDigest();
        InputStream in=new FileInputStream("/Users/mw/temp/w");
        System.out.println(digest.getSHA1(in));
    }
}
