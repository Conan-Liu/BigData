package com.conan.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashMd5 extends UDF {
    private static MessageDigest sha1 = null;

    public String evaluate(String pass) {
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
            sha1.update(pass.getBytes());
            return sha1.digest().toString();
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }
}
