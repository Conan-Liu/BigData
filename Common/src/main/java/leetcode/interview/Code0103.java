package leetcode.interview;

/**
 *
 */
public class Code0103 {

    private static String str="Mr John Smith    ";
    private static int len=13;

    // 直接使用String方法替换
    private static void replace(){
        String s=str.substring(0,len).replace(" ","%20");
        System.out.println(s);
    }

    private static void replaceChar(){
        char[] cs=new char[len*3];
        int i=0,j=0;
        char c;
        while (i<len){
            c=str.charAt(i);
            if(c== ' '){
                cs[j++]='%';
                cs[j++]='2';
                cs[j++]='0';
            }else{
                cs[j++]=c;
            }
            i++;
        }
        String sss=new String(cs,0,j);
        System.out.println(sss);
    }

    public static void main(String[] args) {
        replace();
        replaceChar();
    }
}
