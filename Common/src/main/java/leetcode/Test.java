package leetcode;

import java.math.BigDecimal;

/**
 *
 */
public class Test {

    /**
     * 大数相加
     * 可以直接{@link BigDecimal}
     */
    public static void addTwoBigNum(String s1,String s2) {

        char[] s1Arr = s1.toCharArray();
        char[] s2Arr = s2.toCharArray();

        int s1Len = s1Arr.length;
        int s2Len = s2Arr.length;
        StringBuilder sb = new StringBuilder();

        int cIdx;
        boolean flag = false;
        for (int i = 0, j = 0; i < s1Len || j < s2Len; i++, j++) {
            if (i < s1Len && j < s2Len)
                cIdx = (s1Arr[s1Len - i - 1] - '0') + (s2Arr[s2Len - j - 1] - '0');
            else if (i < s1Len)
                cIdx = s1Arr[s1Len - i - 1] - '0';
            else
                cIdx = s2Arr[s2Len - j - 1] - '0';
            if (flag) {
                cIdx += 1;
            }
            if (cIdx >= 10) {
                flag = true;
            } else {
                flag = false;
            }
            sb.append(cIdx % 10);
        }
        if (flag)
            sb.append(1);

        System.out.println(sb.reverse());
        // System.out.println(Integer.parseInt(s1) + Integer.parseInt(s2));
    }

    public static void main(String[] args) {

        String s1 = "12345678987654321234567876543";
        String s2 = "9786756432534657684364757866564525364587674563455646758676345564765876";

        addTwoBigNum(s1,s2);

        BigDecimal bd1=new BigDecimal(s1);
        BigDecimal bd2=new BigDecimal(s2);

        System.out.println(bd1.add(bd2));
    }
}
