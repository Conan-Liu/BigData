package leetcode.interview;

/**
 * 字符串轮转。给定两个字符串s1和s2，请编写代码检查s2是否为s1旋转而成（比如，waterbottle是erbottlewat旋转后的字符串）。
 * 示例1:
 * 输入：s1 = "waterbottle", s2 = "erbottlewat"
 * 输出：True
 * 示例2:
 * 输入：s1 = "aa", s2 = "aba"
 * 输出：False
 */
public class Code0109 {

    private static String s1 = "asdfasdf";
    private static String s2 = "asdfasdf";

    public static boolean isFlipedString(String s1, String s2) {
        int len1 = s1.length();
        int len2 = s2.length();
        if (len1 != len2) {
            return false;
        }

        return (s1 + s2).contains(s1);
    }

    public static void main(String[] args) {

    }
}
