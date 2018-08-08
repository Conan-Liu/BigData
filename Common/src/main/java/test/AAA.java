package test;

/**
 * Created by Administrator on 2017/4/25.
 */
public class AAA {

    public static int[] a = new int[1 + 10 / 32];

    public static void main(String[] args) throws InterruptedException {
        String[] s=new String[]{"a","b","c","d","e","f"};
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < s.length; i += 2) {
            sb.append("\"").append(s[i]).append("\":\"").append(s[i + 1]).append("\"").append(",");
        }
        System.out.println(sb.length());
        sb.deleteCharAt(sb.length()-1);
        System.out.println(sb);
    }
}
