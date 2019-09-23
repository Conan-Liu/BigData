package com.conan.bigdata.common.util;

import java.util.*;

/**
 */
public class GeoHash {

    public static final double MIN_LAT = -90;
    public static final double MAX_LAT = 90;
    public static final double MIN_LNG = -180;
    public static final double MAX_LNG = 180;

    // 经纬度单独编码长度
    private static int numbits = 3 * 5;

    private static double minLat;
    private static double minLng;
    private static final char[] digits = {'0', '1', '2', '3', '4', '5', '6', '7', '8',
            '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p',
            'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

    // 定义编码映射关系
    private static final Map<Character, Integer> lookUp = new HashMap<>();

    // 初始化编码映射内容
    static {
        int i = 0;
        for (char c : digits) {
            lookUp.put(c, i++);
        }
    }

    public GeoHash() {
        minLat = MAX_LAT - MIN_LAT;
        for (int i = 0; i < numbits; i++) {
            minLat /= 2.0;
        }

        minLng = MAX_LNG - MIN_LNG;
        for (int i = 0; i < numbits; i++) {
            minLng /= 2.0;
        }
    }

    public String encode(double lat, double lng) {
        BitSet latBits = getBits(lat, -90, 90);
        BitSet lngBits = getBits(lng, -180, 180);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numbits; i++) {
            builder.append((lngBits.get(i)) ? '1' : '0');
            builder.append((latBits.get(i)) ? '1' : '0');
        }
        String code = base32(Long.parseLong(builder.toString(), 2));
        return code;
    }

    // 查找周围 8 个点， 解决边界问题
    public List<String> getArroundGeoHash(double lat, double lng) {
        List<String> list = new ArrayList<>();
        double uplat = lat + minLat;
        double downLat = lat - minLat;

        double leftlng = lng - minLng;
        double rightLng = lng + minLng;

        String leftUp = encode(uplat, leftlng);
        list.add(leftUp);

        String leftMid = encode(lat, leftlng);
        list.add(leftMid);

        String leftDown = encode(downLat, leftlng);
        list.add(leftDown);

        String midUp = encode(uplat, lng);
        list.add(midUp);

        String midMid = encode(lat, lng);
        list.add(midMid);

        String midDown = encode(downLat, lng);
        list.add(midDown);

        String rightUp = encode(uplat, rightLng);
        list.add(rightUp);

        String rightMid = encode(lat, rightLng);
        list.add(rightMid);

        String rightDown = encode(downLat, rightLng);
        list.add(rightDown);

        return list;
    }

    private BitSet getBits(double lat, double floor, double ceiling) {
        BitSet buffer = new BitSet(numbits);
        for (int i = 0; i < numbits; i++) {
            double mid = (floor + ceiling) / 2;
            if (lat >= mid) {
                buffer.set(i);
                floor = mid;
            } else {
                ceiling = mid;
            }
        }
        return buffer;
    }

    // 将经纬度合并后的二进制进行指定的32位编码
    // JDK 有base64， 没有base32， 坑
    private String base32(long i) {
        char[] buf = new char[65];
        int charPos = 64;
        boolean negative = (i < 0);
        if (!negative) {
            i = -i;
        }
        while (i <= -32) {
            buf[charPos--] = digits[(int) (-(i % 32))];
            i /= 32;
        }
        buf[charPos] = digits[(int) (-i)];
        if (negative) {
            buf[--charPos] = '-';
        }
        return new String(buf, charPos, (65 - charPos));
    }

    private double decode(BitSet bs, double floor, double ceiling) {
        double mid = 0;
        for (int i = 0; i < bs.length(); i++) {
            mid = (floor + ceiling) / 2;
            if (bs.get(i))
                floor = mid;
            else
                ceiling = mid;
        }
        return mid;
    }

    //对编码后的字符串解码
    public double[] decode(String geohash) {
        StringBuilder buffer = new StringBuilder();
        for (char c : geohash.toCharArray()) {
            int i = lookUp.get(c) + 32;
            buffer.append(Integer.toString(i, 2).substring(1));
        }

        BitSet lonset = new BitSet();
        BitSet latset = new BitSet();

        //偶数位，经度
        int j = 0;
        for (int i = 0; i < numbits * 2; i += 2) {
            boolean isSet = false;
            if (i < buffer.length())
                isSet = buffer.charAt(i) == '1';
            lonset.set(j++, isSet);
        }

        //奇数位，纬度
        j = 0;
        for (int i = 1; i < numbits * 2; i += 2) {
            boolean isSet = false;
            if (i < buffer.length())
                isSet = buffer.charAt(i) == '1';
            latset.set(j++, isSet);
        }

        double lon = decode(lonset, -180, 180);
        double lat = decode(latset, -90, 90);

        return new double[]{lat, lon};
    }

    public static void main(String[] args) {
        GeoHash geohash = new GeoHash();
        String s = geohash.encode(39.923201, 116.390705);
        System.out.println(s);
        geohash.getArroundGeoHash(39.923201, 116.390705);
        double[] geo = geohash.decode(s);
        System.out.println(geo[0] + " " + geo[1]);
    }
}