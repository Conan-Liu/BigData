package test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.UUID;

public class AAA {

    // 可变长参数
    public static void printStrings(String... args) {
        for (String arg : args) {
            System.out.println(arg);
        }
    }

    public static int[] a = new int[1 + 10 / 32];

    public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException, ParseException {
        System.out.println(0.2f == 0.2);

        // URL 编码屏蔽特殊字符
        System.out.println(URLEncoder.encode("%{module}", "UTF-8"));

        // UUID 唯一标识符， 目前全球通用的是微软（GUID）的 8-4-4-4-12 的标准, 占36个字符
        String uuid=UUID.randomUUID().toString();
        System.out.println(uuid);
        System.out.println(uuid.getBytes().length);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        System.out.println(format.parse("2019-01-01").getTime());

        System.out.println(-1 ^ (-1 << 12));

        printStrings("hello", "world", "java");

        System.out.println(Math.pow(2, -2));

        System.out.println(100/200/365);
    }
}
