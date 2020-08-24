package leetcode.interview;

/**
 * 字符串有三种编辑操作:插入一个字符、删除一个字符或者替换一个字符。 给定两个字符串，编写一个函数判定它们是否只需要一次(或者零次)编辑
 * 输入:
 * first = "pale"
 * second = "ple"
 * 输出: True
 * 输入:
 * first = "pales"
 * second = "pal"
 * 输出: False
 */
public class Code0105 {

    private static String s1 = "pales";
    private static String s2 = "pal";

    // 一次循环遍历两个字符串，按字符比较
    private static boolean code() {
        int len1 = s1.length();
        int len2 = s2.length();
        if (len1 - len2 > 1 || len2 - len1 > 1)
            return false;
        int i=0,j=0;
        int cnt=0;
        while (i<len1&&j<len2){
            if(s1.charAt(i)!=s2.charAt(j)){
                cnt++;
                if(cnt>1){
                    return false;
                }
                if(len1>len2){
                    i++;
                }else if(len1<len2){
                    j++;
                }else{
                    i++;
                    j++;
                }
            }else{
                i++;
                j++;
            }
        }
        return true;
    }

    //
    public static void main(String[] args) {
        System.out.println(code());
    }
}
