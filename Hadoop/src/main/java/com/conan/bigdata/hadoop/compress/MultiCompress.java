package com.conan.bigdata.hadoop.compress;

import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by Administrator on 2019/1/14.
 */
public class MultiCompress extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            int result = ToolRunner.run(HadoopConf.getInstance(), new MultiCompress(), args);
            System.exit(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * optType     [compress, uncompress]
     * codeClass   [org.apache.hadoop.io.compress.GzipCodec, org.apache.hadoop.io.compress.BZip2Codec, org.apache.hadoop.io.compress.SnappyCodec]
     * sourceDir   [文件]
     * sourceType  [源数据后缀]
     * targetType  [目标数据后缀]
     *
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String[] argsSplit = new GenericOptionsParser(conf, args).getRemainingArgs();
        String optType = argsSplit[0];
        String codeClass = argsSplit[1];
        String sourceDir = argsSplit[2];
        String sourceType = argsSplit[3];
        String targetType = argsSplit[4];
//        String optType = "compress";
//        String codeClass = "org.apache.hadoop.io.compress.BZip2Codec";
//        String sourceDir = "/repository/kafka/wx_user_track_user_tag/2019-01-13/2019.01.13.log";
//        String sourceType = "log";
//        String targetType = "gz";

        if ("compress".equals(optType)) {
            compress(codeClass, sourceDir, conf, sourceType, targetType);
            return 0;
        } else if ("uncompress".equals(optType)) {
            return 0;
        } else {
            throw new Exception("该命令操作类型不对，只能compress或uncompress !");
        }
    }

    public void compress(String codeClass, String sourceDir, Configuration conf, String sourceType, String targetType) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path sourcePath = new Path(sourceDir);

        Class clz = Class.forName(codeClass);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(clz, conf);

        if (fs.isDirectory(sourcePath)) {
            throw new Exception("只能压缩文件");
        }

        compressFile(sourcePath, fs, codec, conf, sourceType, targetType);
    }

    private void compressFile(Path path, FileSystem fs, CompressionCodec codec, Configuration conf, String sourceType, String targetType) {
        if (path.getName().indexOf(sourceType) > 0) {
            FSDataInputStream input = null;
            CompressionOutputStream compressOut = null;
            try {
                input = fs.open(path);
                System.out.println(String.format("开始压缩: [%s]", path.toString()));
                FSDataOutputStream output = fs.create(new Path(path.toString().replace(sourceType, targetType)), () -> System.out.print("*"));
                compressOut = codec.createOutputStream(output);
                IOUtils.copyBytes(input, compressOut, conf);
                System.out.println("完成\n");
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(input);
                IOUtils.closeStream(compressOut);
            }
        }
    }
}