package test;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * Created by Administrator on 2017/3/28.
 */
public class AAA {
    public static void main(String[] args) throws IOException {

        FileSystem fs = FileSystem.get(null);
        fs.append(null);
    }
}
