package com.conan.bigdata.datamine.cf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ItemCF1 {

    // 余弦相似度计算物品之间的相似度
    private static double[][] getCosSimilarity(int[][] source) {
        // 记录余弦相似度的分母
        System.out.println("计算余弦相似度分母");
        int[] items = new int[source[0].length];
        for (int j = 0; j < source[0].length; j++) {
            for (int i = 0; i < source.length; i++) {
                items[j] += (source[i][j] > 0 ? 1 : 0);
            }
        }
        System.out.println(Arrays.toString(items));
        System.out.println("计算余弦相似度分子");
        Map<String, Double> itemToItem = new HashMap<>();

        for (int i = 0; i < source.length; i++) {
            for (int j1 = 0; j1 < source[0].length; j1++) {
                if (source[i][j1] > 0) {
                    for (int j2 = 0; j2 < source[0].length; j2++) {
                        if (j1 == j2 || source[i][j2] == 0)
                            continue;
                        itemToItem.put(j1 + "-" + j2, (itemToItem.get(j1 + "-" + j2) == null ? 0 : itemToItem.get(j1 + "-" + j2)) + 1);
                    }
                }
            }
        }

        for (Map.Entry<String, Double> k : itemToItem.entrySet()) {
            System.out.println(k.getKey() + "\t" + k.getValue());
        }

        System.out.println("计算物品相似度");
        double[][] simi = new double[source[0].length][source[0].length];
        for (Map.Entry<String, Double> k : itemToItem.entrySet()) {
            String[] keys = k.getKey().split("-");
            itemToItem.put(k.getKey(), k.getValue() / Math.sqrt(items[Integer.parseInt(keys[0])] * items[Integer.parseInt(keys[1])]));
        }
        for (Map.Entry<String, Double> k : itemToItem.entrySet()) {
            System.out.println(k.getKey() + "\t" + k.getValue());
            String[] keys = k.getKey().split("-");
            simi[Integer.parseInt(keys[0])][Integer.parseInt(keys[1])] = k.getValue();
        }

        return simi;
    }

    // 相似度归一化
    // 得到的相似度矩阵，除以每一行的最大值，做归一化
    private static void normalizationSimi(double[][] simi) {
        System.out.println("归一化之前...");
        for (int i = 0; i < simi.length; i++) {
            for (int j = 0; j < simi[0].length; j++) {
                System.out.print(simi[i][j] + "\t");
            }
            System.out.println();
        }
        double[] maxW = new double[simi.length];
        Arrays.fill(maxW, 0);
        for (int i = 0; i < simi.length; i++) {
            for (int j = 0; j < simi[0].length; j++) {
                if (maxW[i] < simi[i][j])
                    maxW[i] = simi[i][j];
            }
        }

        for (int i = 0; i < simi.length; i++) {
            for (int j = 0; j < simi[0].length; j++) {
                simi[i][j] /= maxW[i];
            }
        }

        System.out.println("归一化之后...");
        for (int i = 0; i < simi.length; i++) {
            for (int j = 0; j < simi[0].length; j++) {
                System.out.print(simi[i][j] + "\t");
            }
            System.out.println();
        }
    }

    public static void main(String[] args) {
        // 数据评分矩阵， 行表示用户id， 列表示物品id
        // 0 1 2 3 4 表示行用户A B C D E
        // 0 1 2 3 4 表示列物品a b c d e
        int[][] rating = {{1, 1, 0, 1, 0},
                {0, 1, 1, 0, 1},
                {0, 0, 1, 1, 0},
                {0, 1, 1, 1, 0},
                {1, 0, 0, 1, 0}};

        double[][] simi = getCosSimilarity(rating);

        normalizationSimi(simi);
    }
}