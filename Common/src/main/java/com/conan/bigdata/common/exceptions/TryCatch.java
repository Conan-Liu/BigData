package com.conan.bigdata.common.exceptions;

import java.io.IOException;

/**
 */
public class TryCatch {

    public static void main(String[] args) throws Exception {
//        try {
//            throw new Exception("aaaaaaaaaaa");
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.out.println("bbbbbb");
//        }
//        System.out.println("cccccccccccccc");
        try {

            testThrowException();
        }catch (RuntimeException e){
            System.out.println("ccccccccccccccc");
        }
    }


    public static void testThrowException(){
        try{
            throw new IOException("IOIOIO");
        }catch (IOException e){
            e.printStackTrace();
            throw new RuntimeException("IOIOIO_1");
        }
    }
}