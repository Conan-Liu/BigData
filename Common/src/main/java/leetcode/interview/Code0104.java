package leetcode.interview;

/**
 * 给定一个字符串，编写一个函数判定其是否为某个回文串的排列之一
 * 回文串是指正反两个方向都一样的单词或短语。排列是指字母的重新排列
 * 回文串不一定是字典当中的单词
 * 输入："tactcoa"
 * 输出：true（排列有"tacocat"、"atcocta"，等等）
 */
public class Code0104 {

    private static String str = "asfasd";

    // 既然是回文串，要不全部字符个数为偶数，要不有且只能有一个字符个数是奇数，其它都是偶数
    private static boolean check() {
        int len = str.length();
        if (len == 1)
            return true;
        int[] cs = new int[128];
        for (char c : str.toCharArray()) {
            cs[c]++;
        }
        int cnt = 0;
        for (int i = 0; i < cs.length; i++) {
            if (cs[i] % 2 != 0) {
                cnt++;
                if (len % 2 == 0 && cnt > 0) {
                    return false;
                } else if (len % 2 != 0 && cnt > 1) {
                    return false;
                }
            }
        }
        return true;
    }

    public static void main(String[] args) {
        System.out.println(check());
    }
}
