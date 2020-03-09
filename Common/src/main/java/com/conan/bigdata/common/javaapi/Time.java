package com.conan.bigdata.common.javaapi;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Time {
    private static Calendar c1 = Calendar.getInstance();
    private static Calendar c2 = Calendar.getInstance();
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    public static void main(String[] args) throws ParseException {
        c1.setTime(sdf.parse(""));
        c2.setTime(sdf.parse(""));
        System.out.println((c2.getTimeInMillis() - c1.getTimeInMillis()) / 1000);
    }
}
