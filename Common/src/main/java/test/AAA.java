package test;

import org.apache.commons.lang3.time.FastDateFormat;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.text.ParseException;
import java.util.Date;

/**
 * Created by Administrator on 2017/4/25.
 */
public class AAA {

    public static int[] a = new int[1 + 10 / 32];

    public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException, ParseException {
        FastDateFormat format=FastDateFormat.getInstance("yyyyMMddHHmmss");
        FastDateFormat format1=FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        System.out.println(format1.parse("2018-09-21 17:08:29").getTime());


        System.out.println(String.format("%010d", 102));


        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update("abcdefg".getBytes());
            byte[] b = md.digest();
            int i;
            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0)
                    i += 256;
                if (i < 16)
                    buf.append("0");
                buf.append(Integer.toHexString(i));
            }
            System.out.println(buf);
            System.out.println(buf.toString().substring(0, 6) + buf.toString().substring(26, 32));
        } catch (Exception e) {
            e.printStackTrace();
        }


        System.out.println("abcdefghigklmn".substring(0,10));
    }
}
