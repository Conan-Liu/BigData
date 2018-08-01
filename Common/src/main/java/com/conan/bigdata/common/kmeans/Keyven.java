package com.conan.bigdata.common.kmeans;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Keyven {

    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        /*System.out.println("样本点个数 : ");
        int n = input.nextInt();
        System.out.println("样本点的维度大小 : ");
        int arr_len = input.nextInt();*/
        int n = 19, arr_len = 2;
        List<Point> samples = new ArrayList<Point>();

        for (int i = 0; i < n; i++) {
            double[] x = new double[arr_len];
            System.out.println("点 : ");
            String xyz = input.nextLine();
            String[] dimensions = xyz.split("\\s+");
            for (int j = 0; j < arr_len; j++)
                x[j] = Integer.parseInt(dimensions[j]);
            String text = "Point -- " + (i + 1);
            Point p = new Point(x, arr_len, true, i + 1);
            p.setText(text);
            samples.add(p);
        }

        Kmeans km = new Kmeans(samples, 3, arr_len, 1000);
        km.run();

        input.close();
    }

}
