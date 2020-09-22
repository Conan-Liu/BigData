package leetcode.offer;

/**
 * 剑指 Offer 29. 顺时针打印矩阵
 * 输入一个矩阵，按照从外向里以顺时针的顺序依次打印出每一个数字。
 * 示例 1：
 * 输入：matrix = [[1,2,3],[4,5,6],[7,8,9]]
 * 输出：[1,2,3,6,9,8,7,4,5]
 * 示例 2：
 * 输入：matrix = [[1,2,3,4],[5,6,7,8],[9,10,11,12]]
 * 输出：[1,2,3,4,8,12,11,10,9,5,6,7]
 */
public class Code29 {

    public int[] spiralOrder(int[][] matrix) {
        if (matrix.length == 0) {
            return new int[0];
        }
        int left = 0, right = matrix[0].length - 1, top = 0, bottom = matrix.length - 1, x = 0;
        int[] nums = new int[(right + 1) * (bottom + 1)];

        while (true) {
            for (int i = left; i <= right; i++)
                nums[x++] = matrix[top][i];
            top++;
            if (top > bottom)
                break;

            for (int i = top; i <= bottom; i++)
                nums[x++] = matrix[i][right];
            right--;
            if (left > right)
                break;

            for (int i = right; i >= left; i--)
                nums[x++] = matrix[bottom][i];
            bottom--;
            if (top > bottom)
                break;

            for (int i = bottom; i >= top; i--)
                nums[x++] = matrix[i][left];
            left++;
            if (left > right)
                break;
        }
        return nums;
    }

}
