package test;

import org.apache.commons.lang3.time.FastDateFormat;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

/**
 * Created by Administrator on 2017/4/25.
 */
public class AAA {

    public static void printStrings(String... args){
        for(String arg:args){
            System.out.println(arg);
        }
    }

    public static int[] a = new int[1 + 10 / 32];

    public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException, ParseException {
        System.out.println(0.2f == 0.2);

        // URL 编码屏蔽特殊字符
        System.out.println(URLEncoder.encode("%{module}", "UTF-8"));

        // UUID 唯一标识符， 目前全球通用的是微软（GUID）的 8-4-4-4-12 的标准, 占36个字符
        System.out.println(UUID.randomUUID().toString().getBytes().length);

        SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd");
        System.out.println(format.parse("2019-01-01").getTime());

        System.out.println(-1 ^ (-1 << 12));

        printStrings("hello","world","java");

    }
}
