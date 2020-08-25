package leetcode.interview;

/**
 * 字符串压缩。利用字符重复出现的次数，编写一种方法，实现基本的字符串压缩功能。比如，字符串aabcccccaaa会变为a2b1c5a3。若“压缩”后的字符串没有变短，则返回原先的字符串。你可以假设字符串中只包含大小写英文字母（a至z）。
 * 示例1:
 * 输入："aabcccccaaa"
 * 输出："a2b1c5a3"
 * 示例2:
 * 输入："abbccd"
 * 输出："abbccd"
 * 解释："abbccd"压缩后为"a1b2c2d1"，比原字符串长度更长。
 */
public class Code0106 {

    private static String str = "abc";

    // 固定第一个字符，后面的下标向后移动，两层循环，时间复杂度O(n)
    public static String compressString(String s) {
        int len = s.length();
        StringBuilder sb = new StringBuilder();
        char[] chars = s.toCharArray();
        int i = 0;
        while (i < chars.length) {
            int cnt = 0;
            char c = chars[i];
            cnt++;
            i++;
            while (i < chars.length && chars[i] == c) {
                i++;
                cnt++;
            }
            sb.append(c).append(cnt);
        }
        String ss = sb.toString();
        if (ss.length() >= len) {
            return s;
        } else {
            return ss;
        }
    }

    // 两两字符比较并同步向后移动
    public static String cs(String s) {
        if (s == null || s.length() <= 2) {
            return s;
        }
        StringBuilder sb = new StringBuilder();
        int cnt = 1;
        for (int i = 0; i < s.length(); i++) {
            if (i < s.length() - 1 && s.charAt(i) == s.charAt(i + 1)) {
                cnt++;
            } else {
                sb.append(s.charAt(i)).append(cnt);
                cnt = 1;
            }
        }
        String ss = sb.toString();
        if (ss.length() >= s.length()) {
            return s;
        } else {
            return ss;
        }
    }

    public static void main(String[] args) {
        System.out.println(compressString(str));
        System.out.println(cs(str));
    }
}
