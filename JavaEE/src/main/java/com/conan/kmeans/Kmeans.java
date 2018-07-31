package com.conan.kmeans;

import java.util.*;

public class Kmeans {
    public List<Point> samples;    // 样本点Point
    public List<Cluster> clusters = new ArrayList<Cluster>();    // 划分的集群
    public int k;    // K 值， 划分成 K 个簇
    public int arr_len;    // 样本点Point维度数
    public int steps;    // 最大迭代次数

    public Kmeans(List<Point> samples, int k, int arr_len, int steps) {
        this.samples = samples;
        this.k = k;
        this.arr_len = arr_len;
        this.steps = steps;
    }

    // 选取K个中心点，初始化K个簇
    public void firstStep() {
        Set<Point> centers = new HashSet<Point>();     // 存放簇中心Point
        int id = 0;
        while (centers.size() < k) {
            Random rand = new Random();
            //int ti = rand.nextInt(samples.size()) % samples.size();
            int ti = rand.nextInt(samples.size());
            if (centers.contains(samples.get(ti)))
                continue;
            centers.add(samples.get(ti));
            Cluster cluster = new Cluster(id++, samples.get(ti));    // 中心点确定，则确定一个簇
            clusters.add(cluster);
        }

        classify();
    }

    public void secondStep() {
        List<Cluster> newClusters = new ArrayList<Cluster>();
        for (Cluster clu : clusters) {
            double[] tx = new double[arr_len];
            for (Point p : clu.members) {
                for (int i = 0; i < arr_len; i++) {
                    tx[i] += p.x[i];
                }
            }
            for (int i = 0; i < arr_len; i++) {
                tx[i] /= clu.members.size();
            }
            Point newCenter = new Point(tx, arr_len, false, 0);
            Cluster newClu = new Cluster(clu.id, newCenter);
            newClusters.add(newClu);
        }
        clusters.clear();
        clusters = newClusters;

        classify();
    }

    public void classify() {
        for (int i = 0; i < samples.size(); i++) {
            double minDistance = Double.MAX_VALUE;
            int clu_Id = -1;
            for (Cluster clu : clusters) {
                if (samples.get(i).distance(clu.center) < minDistance) {
                    minDistance = samples.get(i).distance(clu.center);
                    clu_Id = clu.id;
                }
            }

            for (int j = 0; j < clusters.size(); j++) {
                if (clusters.get(j).id == clu_Id) {
                    clusters.get(j).members.add(samples.get(i));
                    break;
                }
            }
        }
    }

    public double loss() {
        double sum = 0;

        for (Cluster clu : clusters) {
            for (Point p : clu.members) {
                sum += p.distance(clu.center);
            }
        }

        return sum;
    }

    public void run() {
        firstStep();
        double oldDist = loss();    // 所有点到各自簇中心点Point的距离总和，这样才表示整个样本点离各自的中心点最近
        double newDist = 0;
        for (int i = 0; i < steps; i++) {
            secondStep();    // 算新的中心点Point, 然后重新聚类
            newDist = loss();    // 重新聚类后，得到新的距离总和
            if (oldDist - newDist < 0.01) {
                break;
            }
            System.out.println("Step " + i + ":" + (oldDist - newDist));
            oldDist = newDist;
        }

        for (int i = 0; i < clusters.size(); i++) {
            System.out.println("第 " + i + " 个簇");
            for (Point p : clusters.get(i).members) {
                if (!p.isSample)
                    continue;
                System.out.print("(");
                for (int xi = 0; xi < p.x.length; xi++) {
                    if (xi != 0)
                        System.out.print(", ");
                    System.out.print(p.x[xi]);
                }
                System.out.print(")");
                System.out.println("\t" + p.text);
            }
        }
    }
}
