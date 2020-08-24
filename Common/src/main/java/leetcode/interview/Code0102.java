package leetcode.interview;

import java.util.Arrays;

/**
 * 给定两个字符串 s1 和 s2，请编写一个程序，确定其中一个字符串的字符重新排列后，能否变成另一个字符串
 * 输入: s1 = "abc", s2 = "bca"
 * 输出: true
 * 输入: s1 = "abc", s2 = "bad"
 * 输出: false
 */
public class Code0102 {

    private static String s1="abcdd";
    private static String s2="dadcb";

    // 字符排序后比较
    private static boolean sortCheck(){
        char[] c1=s1.toCharArray();
        char[] c2=s2.toCharArray();
        Arrays.sort(c1);
        Arrays.sort(c2);
        return new String(c1).equals(new String(c2));
    }

    // 遍历单个字符串，使用数组记录字符串字符下表的出现次数依次递增，再遍历另外一个字符串，依次递减，遍历完全部为0则表示可以重排相同
    private static boolean check(){
        int len1=s1.length();
        int len2=s2.length();
        if(len1!=len2)
            return false;

        // ascii共128个字符
        int[] count=new int[128];
        for(int i=0;i<len1;i++){
            count[s1.charAt(i)]++;
            count[s2.charAt(i)]--;
        }
        for(int i=0;i<128;i++){
            if(count[i]!=0){
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        System.out.println(sortCheck());
        System.out.println(check());
    }
}
