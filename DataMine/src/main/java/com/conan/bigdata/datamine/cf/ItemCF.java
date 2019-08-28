package com.conan.bigdata.datamine.cf;

import java.util.*;

/**
 * 描述：对电影点评过用1表示，0代表没点评过。
 * 给目标用户推荐相似度最高用户喜欢的电影
 */
public class ItemCF {

    // 用户列表
    private static String[] users = {"小明", "小花", "小美", "小张", "小李"};
    // 这些用户相关的电影
    private static String[] movies = {"电影1", "电影2", "电影3", "电影4", "电影5", "电影6", "电影7"};
    // 用户点评电影打星数据， 是users针对movies的评分
    private static Integer[][] allUserMovieCommentList = {
            {1, 1, 1, 0, 1, 0, 0},
            {0, 1, 1, 0, 0, 1, 0},
            {1, 0, 1, 1, 1, 1, 1},
            {1, 1, 1, 1, 1, 0, 0},
            {1, 1, 0, 1, 0, 1, 1}
    };
    // 用户点评电影情况， 行转列, 目的是得到商品 -> 用户的矩阵
    private static Integer[][] allMovieCommentList = new Integer[movies.length][users.length];
    // 电影相似度
    private static Map<String, Double> movieABSimilaritys = new HashMap<>();
    // 待推荐电影相似度列表
    private static Map<Integer, Double> movieSimilaritys = new HashMap<>();
    // 用户所在的位置
    private static int targetUserIndex;
    // 目标用户点评过的电影
    private static List<Integer> targetUserCommentedMovies = new ArrayList<>();
    // 推荐电影
    private static List<Map.Entry<Integer, Double>> recommendList = null;

    public static void main(String[] args) {
        targetUserIndex = getUserIndex("小李");
        if (targetUserIndex < 0) {
            System.out.println("该人不存在");
            System.exit(1);
        }
        // 转换目标用户电影点评列表
        targetUserCommentedMovies = Arrays.asList(allUserMovieCommentList[targetUserIndex]);
        // 计算电影相似度
        calcAllMovieSimilaritys();
        // 获取全部待推荐电影
        calcRecommendMovie();
        // 输出推荐电影
        System.out.println("推荐电影列表: ");
        for (Map.Entry<Integer, Double> item : recommendList) {
            System.out.println(movies[item.getKey()] + " : "+item.getValue());
        }
    }

    // 查询用户所在位置
    private static int getUserIndex(String user) {
        for (int i = 0; i < users.length; i++) {
            if (user.equals(users[i]))
                return i;
        }
        return -1;
    }

    /**
     * 获取全部推荐电影
     */
    private static void calcRecommendMovie() {
        for (int i = 0; i < targetUserCommentedMovies.size() - 1; i++) {
            for (int j = i + 1; j < targetUserCommentedMovies.size(); j++) {
                Double similarity = null;
                if (targetUserCommentedMovies.get(i) == 1 && targetUserCommentedMovies.get(j) == 0 && (movieABSimilaritys.get(i + "" + j) != null || movieABSimilaritys.get(j + "" + i) != null)) {
                    similarity = movieABSimilaritys.get(i + "" + j) != null ? movieABSimilaritys.get(i + "" + j) : movieABSimilaritys.get(j + "" + i);
                    movieSimilaritys.put(j, similarity);
                } else if (targetUserCommentedMovies.get(i) == 0 && targetUserCommentedMovies.get(j) == 1 && (movieABSimilaritys.get(i + "" + j) != null || movieABSimilaritys.get(j + "" + i) != null)) {
                    similarity = movieABSimilaritys.get(i + "" + j) != null ? movieABSimilaritys.get(i + "" + j) : movieABSimilaritys.get(j + "" + i);
                    movieSimilaritys.put(i, similarity);
                }
            }
        }

        recommendList = new ArrayList<>(movieSimilaritys.entrySet());
        Collections.sort(recommendList, new Comparator<Map.Entry<Integer, Double>>() {
            @Override
            public int compare(Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) {
                double d1 = o1.getValue();
                double d2 = o2.getValue();
                if (d1 > d2) {
                    return 1;
                } else if (d1 < d2) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });
        System.out.println("待推荐相似度电影列表 : " + recommendList);
    }

    /**
     * 计算所有电影两两之间的相似度
     */
    private static void calcAllMovieSimilaritys() {
        reverseArray();
        for (int i = 0; i < movies.length - 1; i++) {
            for (int j = i + 1; j < movies.length; j++) {
                movieABSimilaritys.put(i + "" + j, calcTwoMovieSimilarity(allMovieCommentList[i], allMovieCommentList[j]));
            }
        }
        System.out.println("电影相似度 : " + movieABSimilaritys);
    }

    /**
     * 欧几里得距离计算电影相似度
     */
    private static double calcTwoMovieSimilarity(Integer[] movie1Stars, Integer[] movie2Stars) {
        float sum = 0;
        for (int i = 0; i < users.length; i++) {
            sum += Math.pow(movie1Stars[i] - movie2Stars[i], 2);
        }
        return Math.sqrt(sum);
    }

    // 数组行转列
    private static void reverseArray() {
        for (int i = 0; i < movies.length; i++) {
            for (int j = 0; j < users.length; j++) {
                allMovieCommentList[i][j] = allUserMovieCommentList[j][i];
            }
        }
        System.out.println("电影点评行转列 : " + Arrays.deepToString(allMovieCommentList));
    }
}