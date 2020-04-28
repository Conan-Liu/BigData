package com.conan.bigdata.datamine.cf;

import java.util.*;

/**
 * 对电影打星 1 - 5 ， 最低1星，0代表没打星过， 大于平均推荐度代表喜欢
 * 给目标用户推荐相似度最高用户喜欢的电影
 * 1. 计算用户相似度
 * 2. 计算电影的推荐度， 取前几个
 * 3. 去除要推荐的用户看过的电影
 */
public class UserCF {

    // 用户列表
    private static String[] users = {"小明", "小花", "小美", "小张", "小李"};
    // 这些用户相关的电影
    private static String[] movies = {"电影1", "电影2", "电影3", "电影4", "电影5", "电影6", "电影7"};
    // 用户点评电影打星数据， 是users针对movies的评分
    private static int[][] allUserMovieStarList = {
            {3, 1, 4, 4, 1, 0, 0},
            {0, 5, 1, 0, 0, 4, 0},
            {1, 0, 5, 4, 3, 5, 2},
            {3, 1, 4, 3, 5, 0, 0},
            {5, 2, 0, 1, 0, 5, 5}
    };

    // 相似用户集合
    private static List<List<Object>> targetSimilarityUsers = new ArrayList<>();
    // 推荐所有电影集合
    private static List<String> targetRecommendMovies = new ArrayList<>();
    // 点评过电影集合
    private static List<String> targetCommentedMovies = new ArrayList<>();
    // 用户在电影打星集合中的位置
    private static int targetUserIndex;

    public static void main(String[] args) {
        targetUserIndex = getUserIndex("小李");
        if (targetUserIndex < 0) {
            System.out.println("该人不存在");
            System.exit(1);
        }

        calcUserSimilarity();
        calcRecommendMovie();
        handleRecommendMovies();

        System.out.println("推荐电影列表");
        for (String item : targetRecommendMovies) {
            if (!targetCommentedMovies.contains(item)) {
                System.out.print(item + "\t");
            }
        }
        System.out.println();
    }

    // 查询用户所在位置
    private static int getUserIndex(String user) {
        for (int i = 0; i < users.length; i++) {
            if (user.equals(users[i]))
                return i;
        }
        return -1;
    }

    // 打印集合
    private static void printCollection(List<List<Object>> lists) {
        for (List<Object> list : lists) {
            System.out.println(list.get(0) + "\t" + list.get(1));
        }
    }

    /**
     * 获取两个最像似的用户
     */
    private static void calcUserSimilarity() {
        List<List<Object>> similarityUsers = new ArrayList<>();
        List<Object> similarityUser;
        for (int i = 0; i < users.length; i++) {
            if (i == targetUserIndex) {
                continue;
            }
            similarityUser = new ArrayList<>();
            similarityUser.add(i);
            similarityUser.add(calcTwoUserSimilarity(allUserMovieStarList[i], allUserMovieStarList[targetUserIndex]));
            similarityUsers.add(similarityUser);
        }
        sortCollection(similarityUsers, 1);
        targetSimilarityUsers.add(similarityUsers.get(0));
        targetSimilarityUsers.add(similarityUsers.get(1));
    }

    /**
     * 欧几里得距离 计算用户相似度
     */
    private static double calcTwoUserSimilarity(int[] user1Stars, int[] user2Stars) {
        float sum = 0;
        for (int i = 0; i < movies.length; i++) {
            sum += Math.pow(user1Stars[i] - user2Stars[i], 2);
        }
        return Math.sqrt(sum);
    }

    /**
     * 获取全部推荐电影和推荐分值， 计算平均电影推荐度
     */
    private static void calcRecommendMovie() {
        List<List<Object>> recommendMovies = new ArrayList<>();
        List<Object> recommendMovie;
        double recommandRate = 0, sumRate = 0;
        for (int i = 0; i < movies.length; i++) {
            recommendMovie = new ArrayList<>();
            recommendMovie.add(i);
            recommandRate = allUserMovieStarList[Integer.parseInt(targetSimilarityUsers.get(0).get(0).toString())][i] * Double.parseDouble(targetSimilarityUsers.get(0).get(1).toString())
                    + allUserMovieStarList[Integer.parseInt(targetSimilarityUsers.get(1).get(0).toString())][i] * Double.parseDouble(targetSimilarityUsers.get(1).get(1).toString());
            recommendMovie.add(recommandRate);
            recommendMovies.add(recommendMovie);
            sumRate += recommandRate;
        }
        sortCollection(recommendMovies, -1);
        for (List<Object> item : recommendMovies) {
            if (Double.parseDouble(item.get(1).toString()) > sumRate / movies.length) {
                targetRecommendMovies.add(movies[Integer.parseInt(item.get(0).toString())]);
            }
        }
    }

    /**
     * 把推荐列表中用户已经点评过的电影剔除
     */
    private static void handleRecommendMovies() {
        for (int i = 0; i < movies.length; i++) {
            if (allUserMovieStarList[targetUserIndex][i] != 0) {
                targetCommentedMovies.add(movies[i]);
            }
        }
    }

    /**
     * 集合排序
     * order  1 正序    -1 倒序
     */
    private static void sortCollection(List<List<Object>> list, final int order) {
        Collections.sort(list, new Comparator<List<Object>>() {
            @Override
            public int compare(List<Object> o1, List<Object> o2) {
                if (Double.valueOf(o1.get(1).toString()) > Double.valueOf(o2.get(1).toString())) {
                    return order;
                } else if (Double.valueOf(o1.get(1).toString()) < Double.valueOf(o2.get(1).toString())) {
                    return -order;
                } else {
                    return 0;
                }
            }
        });
    }
}