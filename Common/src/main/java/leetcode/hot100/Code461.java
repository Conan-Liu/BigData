package leetcode.hot100;

/**
 * 两个整数之间的汉明距离指的是这两个数字对应二进制位不同的位置的数目。
 * 给出两个整数 x 和 y，计算它们之间的汉明距离。
 * 注意：
 * 0 ≤ x, y < 231.
 * 示例:
 * 输入: x = 1, y = 4
 * 输出: 2
 * 解释:
 * 1   (0 0 0 1)
 * 4   (0 1 0 0)
 * ↑   ↑
 * 上面的箭头指出了对应二进制位不同的位置。
 */
public class Code461 {

    // 取模
    public static int hammingDistance(int x, int y) {
        int cnt = 0;
        while (x > 0 || y > 0) {
            if (y == 0) {
                if (x % 2 != 0)
                    cnt++;
                x = x / 2;
            } else if (x == 0) {
                if (y % 2 != 0)
                    cnt++;
                y = y / 2;
            } else {
                if (x % 2 != y % 2) {
                    cnt++;
                }
                x = x / 2;
                y = y / 2;
            }
        }

        return cnt;
    }

    // 位运算
    public static int hammingDistance1(int x, int y) {
        int cnt = 0;
        while (x > 0 || y > 0) {
            if (y == 0) {
                if ((x & 1) != 0)
                    cnt++;
                x >>= 1;
            } else if (x == 0) {
                if ((y & 1) != 0)
                    cnt++;
                y >>= 1;
            } else {
                if ((x & 1) != (y & 1)) {
                    cnt++;
                }
                x >>= 1;
                y >>= 1;
            }
        }

        return cnt;
    }


    public static void main(String[] args) {
        System.out.println(hammingDistance1(1, 3));
    }
}
