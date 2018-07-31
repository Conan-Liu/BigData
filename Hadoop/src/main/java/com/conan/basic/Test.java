package com.conan.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;

import java.io.IOException;

/**
 * Created by Administrator on 2017/7/11.
 */
public class Test {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=Job.getInstance(new Configuration(),"");
        job.setJobID(new JobID("liufeiqiang",2123));

        job.waitForCompletion(true);
    }
}
