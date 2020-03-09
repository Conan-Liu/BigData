package com.conan.bigdata.hbase.job;

import java.io.File;
import java.io.FileInputStream;

/**
 * hbase 存取图片
 * 把图片转成二进制数据存入HBase
 */
public class StoreImage {

    private byte[] readPicture(String path) {
        byte[] ppp = null;
        try {
            File f = new File(path);
            FileInputStream fin = new FileInputStream(f);
            ppp = new byte[fin.available()];
            fin.read(ppp);
            fin.close();

            System.out.println(ppp.length);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ppp;
    }
}