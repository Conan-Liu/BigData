package com.conan.bigdata.common.algorithm;

import java.util.Arrays;

/**
 * 矩阵的相关操作
 */
public class MatrixExp {

    // 矩阵
    private static int[][] matrix = new int[][]{
            {1, 2, 3},
            {4, 5, 6}
    };

    // 矩阵的特殊情况，方阵
    private static int[][] matrixSquare = new int[][]{
            {1, 2, 3},
            {4, 5, 6},
            {7, 8, 9}
    };


    // 转置，行列下标互换即可，时间复杂度O(n*m)，空间复杂度O(n*m)
    private static void transpose() {
        int[][] matrix1 = new int[matrix[0].length][matrix.length];
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[0].length; j++) {
                matrix1[j][i] = matrix[i][j];
            }
        }

        for (int i = 0; i < matrix1.length; i++) {
            System.out.println(Arrays.toString(matrix1[i]));
        }
    }

    /**
     * 方阵旋转90度，参考{@link leetcode.interview.Code0107#rotate(int[][])}
     */

    // 矩阵旋转90度，和转置类似，原矩阵的列下标变成新矩阵的行下标，原矩阵的行下标对应新矩阵的列下标，刚好顺序相反
    // 时间复杂度O(n*m)，空间复杂度O(n*m)
    private static void rotate90() {
        int[][] matrix1 = new int[matrix[0].length][matrix.length];
        int len=matrix.length;
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[0].length; j++) {
                matrix1[j][len-i-1]=matrix[i][j];
            }
        }

        for (int i = 0; i < matrix1.length; i++) {
            System.out.println(Arrays.toString(matrix1[i]));
        }
    }

    public static void main(String[] args) {
        transpose();
        rotate90();
    }
}
