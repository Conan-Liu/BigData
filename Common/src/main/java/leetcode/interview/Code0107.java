package leetcode.interview;

import com.conan.bigdata.common.algorithm.MatrixExp;

import java.util.Arrays;

/**
 * 给你一幅由 N × N 矩阵（方阵）表示的图像，其中每个像素的大小为 4 字节。请你设计一种算法，将图像旋转 90 度。
 * 不占用额外内存空间能否做到？
 * 示例 1:
 * 给定 matrix =
 * [
 * [1,2,3],
 * [4,5,6],
 * [7,8,9]
 * ],
 * 原地旋转输入矩阵，使其变为:
 * [
 * [7,4,1],
 * [8,5,2],
 * [9,6,3]
 * ]
 */
public class Code0107 {

    private static int[][] matrix = new int[][]{
            {1, 2, 3},
            {4, 5, 6},
            {7, 8, 9}
    };

    /**
     * 顺时针旋转90度，逆时针逻辑差不多
     */
    // 左上-右下对角线翻转，然后每一行轴对称翻转，时间复杂度O(n^2)，空间复杂度O(1)
    private static void rotate(int[][] matrix) {
        int n = matrix.length;
        // 左上-右下对角线翻转
        for (int i = 0; i < n; i++) {
            for (int j = i; j < n; j++) {
                int tmp = matrix[i][j];
                matrix[i][j] = matrix[j][i];
                matrix[j][i] = tmp;
            }
        }

        // 每一行轴对称翻转
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n / 2; j++) {
                int tmp = matrix[i][j];
                matrix[i][j] = matrix[i][n - j - 1];
                matrix[i][n - j - 1] = tmp;
            }
        }
    }

    private static void rotate1(int[][] matrix) {
        /**
         * 直接定义一个新数组，参考{@link MatrixExp#rotate90()}
         */
    }

    public static void main(String[] args) {
        rotate(matrix);
        for (int i = 0; i < matrix.length; i++) {
            System.out.println(Arrays.toString(matrix[i]));
        }
    }
}
