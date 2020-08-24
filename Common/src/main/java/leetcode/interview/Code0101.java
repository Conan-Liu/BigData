package leetcode.interview;

/**
 * 实现一个算法，确定一个字符串 s 的所有字符是否全都不同
 * 输入: s = "leetcode"
 * 输出: false
 * 输入: s = "abc"
 * 输出: true
 */
public class Code0101 {

    private static String str = "abcde";

    // 首尾indexof判断是否相等，相等则唯一，不相等则重复，时间复杂度O(n^2)
    public static boolean isUnique() {
        for (char c : str.toCharArray()) {
            if (str.indexOf(c) != str.lastIndexOf(c)) {
                return false;
            }
        }
        return true;
    }

    // 判断是否重复，可以使用bitmap，时间复杂度O(n)
    public static boolean bitmapUnique() {
        // ascii字符数是128个，从0开始到127，只需要两个long型数
        long low64 = 0;
        long high64 = 0;
        long index;
        for (char c : str.toCharArray()) {
            if (c >= 64) {
                // 高位的64个字符
                index = 1 << (c - 64);
                if ((high64 & index) != 0) {
                    return false;
                } else {
                    high64 |= index;
                }
            } else {
                // 低位的64个字符
                index = 1 << c;
                if ((low64 & index) != 0) {
                    return false;
                } else {
                    low64 |= index;
                }
            }
        }
        return true;
    }

    public static void main(String[] args) {
        System.out.println(isUnique());
        System.out.println(bitmapUnique());
    }
}
